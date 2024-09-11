from google.cloud import bigquery
import logging
from concurrent.futures import ThreadPoolExecutor
import time

# Configure logging
logging.basicConfig(level=logging.INFO)

def load_csv_from_gcs(dataset_id, table_id, gcs_uri, project_id=None):
    """Loads CSV data from GCS into BigQuery."""
    client = bigquery.Client(project=project_id)

    # Defining the dataset and table in BigQuery
    table_ref = client.dataset(dataset_id).table(table_id)

    # Loading the data
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,  
        skip_leading_rows=1,
        max_bad_records=5,  
        allow_quoted_newlines=True  
    )

    try:
        # Load the data from GCS to BigQuery
        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

        # Wait for the job to complete
        load_job.result()
        logging.info(f"Successfully loaded {gcs_uri} into {dataset_id}.{table_id}")

    except Exception as e:
        logging.error(f"Failed to load {gcs_uri} into {dataset_id}.{table_id}: {e}")


def load_multiple_files_in_parallel(file_list, dataset_id, project_id, max_workers=3):
    """Loads multiple CSV files into BigQuery tables in parallel."""
    start_time = time.time()

    # Use ThreadPoolExecutor for parallelism
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for file_info in file_list:
            future = executor.submit(
                load_csv_from_gcs,
                dataset_id,
                file_info['table_id'],
                file_info['gcs_uri'],
                project_id
            )
            futures.append(future)

        # Wait for all jobs to complete
        for future in futures:
            future.result()

    elapsed_time = time.time() - start_time
    logging.info(f"All files loaded in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    dataset_id = "vehicle_analysis"  
    project_id = "jlr-interview-435215"  

    # List of files to ingest
    files_to_ingest = [
        {"table_id": "base_data", "gcs_uri": "gs://vehicle-inventory/Base_data.csv.csv"},
        {"table_id": "options_data", "gcs_uri": "gs://vehicle-inventory/options_data.csv"},
        {"table_id": "vehicle_line_mapping", "gcs_uri": "gs://vehicle-inventory/vehicle_line_mapping.csv"}
    ]

    # Ingest files in parallel
    load_multiple_files_in_parallel(files_to_ingest, dataset_id, project_id, max_workers=3)
