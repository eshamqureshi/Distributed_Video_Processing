# master.py

from flask import Flask, request, send_file, jsonify
import os
import requests
from werkzeug.utils import secure_filename
from moviepy.editor import VideoFileClip, concatenate_videoclips
from cryptography.fernet import Fernet

app = Flask(__name__)

# Define directories with your specified paths
UPLOAD_DIR = "F:\Semester\Semester 7\\Parallel and distributing computing\\Assignment\\Project1\\video\\uploaded"
CHUNK_DIR = "F:\\Semester\\Semester 7\\Parallel and distributing computing\\Assignment\\Project1\\video\\chunks"
PROCESSED_DIR = "F:\\Semester\\Semester 7\\Parallel and distributing computing\\Assignment\\Project1\\video\\processed"
DOWNLOAD_DIR = "F:\\Semester\\Semester 7\\Parallel and distributing computing\\Assignment\\Project1\\video\\downloads"

# Output files
OUTPUT_FILE = "F:\\Semester\\Semester 7\\Parallel and distributing computing\\Assignment\\Project1\\video\\encoded\\final_encoded.mp4"
ENCRYPTED_OUTPUT_FILE = "F:\\Semester\\Semester 7\\Parallel and distributing computing\\Assignment\\Project1\\video\\encrypted\\final_encoded_encrypted.mp4"
DECODED_OUTPUT_FILE = "F:\\Semester\\Semester 7\\Parallel and distributing computing\\Assignment\\Project1\\video\\decoded\\final_decoded.mp4"

# Worker URLs
WORKER1_URL = "http://localhost:5001/process_chunk"  # Worker 1
WORKER2_URL = "http://localhost:5002/process_chunk"  # Worker 2

# Hardcoded Encryption Key (Fernet key)
ENCRYPTION_KEY = b'aWA1pt7BBDR2vHPdkuv1a73OMQNIByvpHPfPpCWccn4='  # Your provided key
cipher_suite = Fernet(ENCRYPTION_KEY)



@app.route('/')
def index():
    return app.send_static_file("index.html")

@app.route('/upload', methods=['POST'])
def upload():
    try:
        video_file = request.files['video']
        filename = secure_filename(video_file.filename)
        video_path = os.path.join(UPLOAD_DIR, filename)
        video_file.save(video_path)
        return jsonify({"status": "success", "video_path": video_path})
    except Exception as e:
        print(f"Upload Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/process_video', methods=['POST'])
def process_video():
    try:
        action = request.form['action']
        video_filename = request.form['video_filename']
        video_path = os.path.join(UPLOAD_DIR, video_filename)

        if not os.path.exists(video_path):
            return jsonify({"status": "error", "message": "Video file not found."}), 404

        if action == "encode":
            # Split the video into chunks
            split_video(video_path)

            # Get list of chunk files sorted to maintain order
            chunk_files = sorted(os.listdir(CHUNK_DIR))
            processed_chunks = []

            for i, chunk_name in enumerate(chunk_files):
                chunk_path = os.path.join(CHUNK_DIR, chunk_name)
                worker_url = WORKER1_URL if i % 2 == 0 else WORKER2_URL

                with open(chunk_path, 'rb') as f:
                    files = {'chunk': f}
                    data = {'action': action}
                    response = requests.post(worker_url, files=files, data=data)

                if response.status_code == 200:
                    # Save the processed chunk
                    processed_chunk_filename = f"processed_{chunk_name}"
                    processed_chunk_path = os.path.join(PROCESSED_DIR, processed_chunk_filename)
                    with open(processed_chunk_path, 'wb') as out_f:
                        out_f.write(response.content)
                    processed_chunks.append(processed_chunk_path)
                    print(f"Processed {chunk_name} and saved as {processed_chunk_filename}.")
                else:
                    error_message = response.json().get('error', 'Unknown error')
                    return jsonify({"status": "error", "message": f"Worker {i % 2 + 1} failed: {error_message}"}), 500

            # Combine the processed chunks into the final video
            combine_chunks(processed_chunks, OUTPUT_FILE)

            # Encrypt the final encoded video
            encrypt_video(OUTPUT_FILE, ENCRYPTED_OUTPUT_FILE)

            # Clean up chunk directories
           # cleanup_directories([CHUNK_DIR, PROCESSED_DIR])

            return jsonify({"status": "success", "output_file": ENCRYPTED_OUTPUT_FILE})

        elif action == "decode":
            # Decrypt the encrypted video
            decrypt_video(video_path, OUTPUT_FILE)

            # Split the decrypted video into chunks
            split_video(OUTPUT_FILE)

            # Get list of chunk files sorted to maintain order
            chunk_files = sorted(os.listdir(CHUNK_DIR))
            processed_chunks = []

            for i, chunk_name in enumerate(chunk_files):
                chunk_path = os.path.join(CHUNK_DIR, chunk_name)
                worker_url = WORKER1_URL if i % 2 == 0 else WORKER2_URL

                with open(chunk_path, 'rb') as f:
                    files = {'chunk': f}
                    data = {'action': action}
                    response = requests.post(worker_url, files=files, data=data)

                if response.status_code == 200:
                    # Save the processed chunk
                    processed_chunk_filename = f"processed_{chunk_name}"
                    processed_chunk_path = os.path.join(PROCESSED_DIR, processed_chunk_filename)
                    with open(processed_chunk_path, 'wb') as out_f:
                        out_f.write(response.content)
                    processed_chunks.append(processed_chunk_path)
                    print(f"Processed {chunk_name} and saved as {processed_chunk_filename}.")
                else:
                    error_message = response.json().get('error', 'Unknown error')
                    return jsonify({"status": "error", "message": f"Worker {i % 2 + 1} failed: {error_message}"}), 500

            # Combine the processed chunks into the decoded video
            combine_chunks(processed_chunks, DECODED_OUTPUT_FILE)

            # Clean up chunk directories
          ##  cleanup_directories([CHUNK_DIR, PROCESSED_DIR])

            return jsonify({"status": "success", "output_file": DECODED_OUTPUT_FILE})

        else:
            return jsonify({"status": "error", "message": "Invalid action. Must be 'encode' or 'decode'."}), 400

    except Exception as e:
        print(f"Process Video Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/download', methods=['GET'])
def download():
    try:
        output_file = request.args.get("file")
        if not output_file:
            return jsonify({"status": "error", "message": "No file specified."}), 400

        # Ensure the file exists
        if not os.path.exists(output_file):
            return jsonify({"status": "error", "message": "Requested file does not exist."}), 404

        return send_file(output_file, as_attachment=True)
    except Exception as e:
        print(f"Download Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def split_video(video_path):
    try:
        video = VideoFileClip(video_path)
        duration = video.duration

        # Decide the number of chunks based on the number of workers
        num_workers = 2
        chunk_duration = duration / num_workers

        # Clear previous chunks
        cleanup_directories([CHUNK_DIR])

        for i in range(num_workers):
            start = i * chunk_duration
            end = (i + 1) * chunk_duration if i < num_workers - 1 else duration
            chunk = video.subclip(start, end)
            chunk_filename = f"chunk_{i}.mp4"
            chunk_path = os.path.join(CHUNK_DIR, chunk_filename)
            chunk.write_videofile(
                chunk_path,
                codec="libx264",
                audio_codec="aac",
                temp_audiofile='temp-audio.m4a',
                remove_temp=True,
                verbose=False,
                logger=None
            )
            print(f"Created {chunk_filename} from {start} to {end} seconds.")
    except Exception as e:
        print(f"Split Video Error: {e}")
        raise

def combine_chunks(chunks, output_file):
    try:
        clips = []
        for chunk in chunks:
            clip = VideoFileClip(chunk)
            clips.append(clip)
            print(f"Added {chunk} to final video.")

        final_clip = concatenate_videoclips(clips)
        final_clip.write_videofile(
            output_file,
            codec="libx264",
            audio_codec="aac",
            ffmpeg_params=["-movflags", "faststart"],
            verbose=False,
            logger=None
        )
        print(f"Final video saved as {output_file}.")

        # Close all clips to free resources
        for clip in clips:
            clip.close()
        final_clip.close()
    except Exception as e:
        print(f"Combine Chunks Error: {e}")
        raise

def encrypt_video(input_file, output_file):
    try:
        with open(input_file, 'rb') as f:
            data = f.read()
        encrypted_data = cipher_suite.encrypt(data)
        with open(output_file, 'wb') as f:
            f.write(encrypted_data)
        print(f"Encrypted video saved as {output_file}.")
    except Exception as e:
        print(f"Encryption Error: {e}")
        raise

def decrypt_video(input_file, output_file):
    try:
        with open(input_file, 'rb') as f:
            encrypted_data = f.read()
        decrypted_data = cipher_suite.decrypt(encrypted_data)
        with open(output_file, 'wb') as f:
            f.write(decrypted_data)
        print(f"Decrypted video saved as {output_file}.")
    except Exception as e:
        print(f"Decryption Error: {e}")
        raise

def cleanup_directories(directories):
    for directory in directories:
        try:
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                os.remove(file_path)
            print(f"Cleaned up directory: {directory}")
        except Exception as e:
            print(f"Cleanup Error in {directory}: {e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
