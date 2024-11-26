from google.cloud import storage
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to a GCS bucket.

    :param bucket_name: Name of the GCS bucket
    :param source_file_name: Local path to the file
    :param destination_blob_name: Name for the file in the bucket
    """
    try:
        # Initialize the GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Upload the file
        logging.info(f"Uploading {source_file_name} to {bucket_name}/{destination_blob_name}...")
        blob.upload_from_filename(source_file_name)
        logging.info(f"File {source_file_name} uploaded to {bucket_name}/{destination_blob_name}.")
    except Exception as e:
        logging.error(f"Failed to upload {source_file_name} to {bucket_name}: {e}")

# Configuration
BUCKET_NAME = "car-sales-data-1"  # Replace with your GCS bucket name
FILES = [
    {"source": "base_data.csv", "destination": "base_data.csv"},
    {"source": "options_data.csv", "destination": "options_data.csv"},
    {"source": "vehicle_line_mapping.csv", "destination": "vehicle_mapping.csv"},
]

# Upload each file
for file in FILES:
    upload_to_gcs(BUCKET_NAME, file["source"], file["destination"])
