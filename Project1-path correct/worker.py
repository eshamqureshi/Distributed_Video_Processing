# worker1.py

from flask import Flask, request, send_file, jsonify
import os
from moviepy.editor import VideoFileClip
from PIL import Image
import numpy as np

app = Flask(__name__)

# Define directories with your specified paths
CHUNKS_DIR = r"F:\\Semester\\Semester 7\\Parallel and distributing computing\\Assignment\\Project1\\video\\chunks"
PROCESSED_CHUNKS_DIR = r"F:\\Semester\\Semester 7\\Parallel and distributing computing\\Assignment\\Project1\\video\\processed"

# Ensure directories exist
os.makedirs(CHUNKS_DIR, exist_ok=True)
os.makedirs(PROCESSED_CHUNKS_DIR, exist_ok=True)

def invert_colors(frame):
    """
    Invert the colors of the given video frame.
    """
    pil_img = Image.fromarray(frame)
    inverted_img = Image.fromarray(255 - np.array(pil_img))
    return np.array(inverted_img)

@app.route('/process_chunk', methods=['POST'])
def process_chunk():
    try:
        # Get action from form data
        action = request.form.get('action')
        if action not in ['encode', 'decode']:
            return jsonify({'error': 'Invalid action. Must be "encode" or "decode".'}), 400

        # Save the received chunk
        chunk_file = request.files['chunk']
        chunk_filename = chunk_file.filename
        chunk_path = os.path.join(CHUNKS_DIR, chunk_filename)
        chunk_file.save(chunk_path)
        print(f"Worker 1: Received and saved chunk: {chunk_filename} for action: {action}")

        # Load the video chunk
        video = VideoFileClip(chunk_path)

        if action == 'encode':
            # Apply color inversion
            processed_video = video.fl_image(invert_colors)
            processed_chunk_filename = f"processed_{chunk_filename}"
            print(f"Worker 1: Encoded (inverted) chunk: {chunk_filename}")
        elif action == 'decode':
            # Reverse color inversion (since inversion is its own inverse)
            processed_video = video.fl_image(invert_colors)
            processed_chunk_filename = f"processed_{chunk_filename}"
            print(f"Worker 1: Decoded (re-inverted) chunk: {chunk_filename}")

        # Save the processed chunk
        processed_chunk_path = os.path.join(PROCESSED_CHUNKS_DIR, processed_chunk_filename)
        processed_video.write_videofile(
            processed_chunk_path,
            codec="libx264",
            audio_codec="aac",
            temp_audiofile='temp-audio.m4a',
            remove_temp=True,
            verbose=False,
            logger=None
        )
        print(f"Worker 1: Saved processed chunk as {processed_chunk_filename}")

        # Close the video clips to free resources
        video.close()
        processed_video.close()

        # Return the processed chunk file
        return send_file(processed_chunk_path, as_attachment=True)

    except Exception as e:
        print(f"Worker 1 Error: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = 5001
    worker_name = "Worker"
    print(f"{worker_name} running on port {port}")
    app.run(port=port, debug=True)
