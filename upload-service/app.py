import os
import requests
import boto3
from flask import Flask,request,jsonify
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

METADATA_API_URL = os.getenv('METADATA_API_URL', 'http://localhost:8080/videos')
MINIO_URL = os.getenv('MINIO_URL', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

@app.route("/upload",methods=["POST"])
def upload_video():
    if "file" not in request.files:
        return jsonify({"error":"No file part in the request"}),400
    file = request.files["file"]
    if file.filename=='':
        return jsonify({"error":"No selected file"}),400
    
    processing_type = request.form.get("processingType")
    if not processing_type or processing_type not in ["transcript","subtitle"]:
        return jsonify({"error":"Invalid or missing processingType. Must be 'transcript' or 'subtitle'"}),400

    video_id = None
    try:
        print(f"Creating metadata record for {file.filename}...")
        metadata_payload = {
            "filename": file.filename,
            "processingType": processing_type
        }

        response = requests.post(METADATA_API_URL,json=metadata_payload)
        response.raise_for_status()
        metadata = response.json()
        video_id = metadata.get("id")
        print(f"Metadata record created with ID: {video_id}")

        print(f"Uploading {file.filename} to MinIO...")
        s3_client = boto3.client(
            "s3",
            endpoint_url = f"http://{MINIO_URL}",
            aws_access_key_id = MINIO_ACCESS_KEY,
            aws_secret_access_key= MINIO_SECRET_KEY,
        )
        s3_client.upload_fileobj(file,"videos",file.filename)
        video_url = f"http://{MINIO_URL}/videos/{file.filename}"
        print("Upload to MinIO complete.")

        print("Uploading metadata record with UPLOADED status...")
        update_payload = {
            "status": "UPLOADED",
            "videoUrl": video_url
        }

        update_url = f"{METADATA_API_URL}/{video_id}"
        update_response = requests.put(update_url,json=update_payload)
        update_response.raise_for_status()
        print("Metadata update complete.")

        return jsonify({"message": "File uploaded and processing started.", "videoId": video_id}), 202
    
    except requests.exceptions.RequestException as e:
        print(f"Error communicating with metadata service: {e}")
        return jsonify({"error":"Failed to process request due to metadata service error."}), 500
    except Exception as e:
        if video_id:
            try:
                requests.put(f"{METADATA_API_URL}/{video_id}",json={"status":"FAILED"})
            except requests.exceptions.RequestException as re:
                print(f"Additionally, failed to update status to FAILED: {re}")
            print(f"An unexpected error ocurred: {e}")
            return jsonify({"error": "An internal error ocurred during upload."}), 500
        
if __name__ == "__main__":
    app.run(host="0.0.0.0",port=5000,debug=True)
    