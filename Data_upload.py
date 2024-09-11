import os
from google.cloud import storage
import pandas as pd

# Function to upload the file to Google Cloud Storage (GCS)
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, project_name=None):
    """Uploads a file to Google Cloud Storage."""
    try:
        if project_name:
            storage_client = storage.Client(project=project_name)
        else:
            storage_client = storage.Client()  # Default project

        print(f"Initialized GCS client for project: {project_name if project_name else 'default project'}")
        
        bucket = storage_client.bucket(bucket_name)
        print(f"Connected to bucket: {bucket_name}")
        
        blob = bucket.blob(destination_blob_name)
        print(f"Preparing to upload file {source_file_name} as {destination_blob_name}...")
        
        # Upload the file to the bucket
        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded successfully to {bucket_name}/{destination_blob_name}.")
    except Exception as e:
        print(f"Error during GCS upload: {e}")

# Function to simulate data ingestion
def ingest_data(file_path, destination_bucket, project_name=None):
    """Simulates data ingestion and uploads the data to GCS."""
    try:
        # Load the dataset from a local file (simulate ingestion)
        print(f"Loading data from {file_path}...")
        base_df = pd.read_csv(file_path)
        print(f"Data loaded successfully. Shape: {base_df.shape}")
        
        # Upload the ingested data to Google Cloud Storage
        upload_to_gcs(destination_bucket, file_path, os.path.basename(file_path), project_name)

    except FileNotFoundError:
        print(f"File {file_path} not found. Please check the path.")
    except pd.errors.ParserError as pe:
        print(f"Error parsing {file_path}: {pe}")
    except Exception as e:
        print(f"Failed to ingest data: {e}")

# Example usage
if __name__ == "__main__":
    # Path to the local dataset file
    file_path = 'Base_data1.csv'  # Ensure this path is correct
    
    # Name of your GCS bucket
    destination_bucket = 'vehicle-inventory'  # Ensure the bucket exists
    
    # Name of your Google Cloud project
    project_name = 'jlr-interview-435215'  # Replace with your project ID
    
    # Start the ingestion process
    print(f"Starting data ingestion for project: {project_name}")
    ingest_data(file_path, destination_bucket, project_name)
