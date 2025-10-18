import os
import pika
import requests
import whisper
import boto3
import json
import time
from botocore.client import Config 

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
MINIO_URL = os.getenv('MINIO_URL', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
METADATA_API_URL = os.getenv('METADATA_API_URL', 'http://localhost:8080/videos')

print("Loading Whisper model...")
model = whisper.load_model("tiny")
print("Whisper model loaded.")

def update_status(video_id, status, output_url=None):
    url = f"{METADATA_API_URL}/{video_id}"
    payload = {"status": status}
    if output_url:
        payload["outputUrl"] = output_url
    try:
        response = requests.put(url, json=payload)
        response.raise_for_status()
        print(f"Successfully updated status for video {video_id} to {status}")
    except requests.exceptions.RequestException as e:
        print(f"Error updating status for video {video_id}: {e}")

def process_video(video_id, filename):
    local_video_path = f"/tmp/{filename}"
    local_transcript_path = f"/tmp/{filename}.txt"
    try:
        update_status(video_id, "PROCESSING")

        print(f"Downloading {filename} from MinIO...")
        s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{MINIO_URL}',
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4') # <<< 2. CORRECT THIS LINE
        )
        s3_client.download_file('videos', filename, local_video_path)
        print("Download complete.")

        print(f"Transcribing {filename}...")
        result = model.transcribe(local_video_path)
        transcript = result['text']
        with open(local_transcript_path, 'w', encoding='utf-8') as f:
            f.write(transcript)
        print("Transcription complete.")

        print(f"Uploading transcript to MinIO...")
        output_filename = f"{filename}.txt"
        s3_client.upload_file(local_transcript_path, 'transcripts', output_filename)
        output_url = f"http://{MINIO_URL}/transcripts/{output_filename}"
        print("Upload complete.")
        update_status(video_id, "COMPLETED", output_url)

    except Exception as e:
        print(f"An error occurred while processing video {video_id}: {e}")
        update_status(video_id, "FAILED")
    finally:
        if os.path.exists(local_video_path):
            os.remove(local_video_path)
        if os.path.exists(local_transcript_path):
            os.remove(local_transcript_path)
        print(f"Cleaned up local files for video {video_id}.")

def callback(ch, method, properties, body):
    message = json.loads(body)
    video_id = message.get('id')
    filename = message.get('filename')
    if not video_id or not filename:
        print("Received invalid message:", message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    print(f"Received job for video ID: {video_id}, filename: {filename}")
    process_video(video_id, filename)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"Finished processing job for video ID: {video_id}")

def main():
    connection = None
    while True:
        try:
            print("Connecting to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.exchange_declare(exchange='video_events', exchange_type='topic')
            result = channel.queue_declare(queue='transcription_queue', durable=True)
            queue_name = result.method.queue
            channel.queue_bind(exchange='video_events', queue=queue_name, routing_key='video.uploaded.transcript')
            print('Waiting for messages. To exit press CTRL+C')
            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Failed to connect to RabbitMQ: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("Interrupted by user. Shutting down.")
            if connection:
                connection.close()
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}. Retrying in 10 seconds...")
            time.sleep(10)

if __name__ == '__main__':
    if not os.path.exists('/tmp'):
        os.makedirs('/tmp')
    main()