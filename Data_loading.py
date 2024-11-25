from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def load_to_bigquery(bucket_name, file_name, dataset_id, table_id):
    """
    Load a CSV file from GCS into BigQuery.

    :param bucket_name: GCS bucket name
    :param file_name: GCS file name (e.g., "enriched_dataset.csv")
    :param dataset_id: BigQuery dataset ID
    :param table_id: BigQuery table ID
    """
    try:
        client = bigquery.Client()
        uri = f"gs://{bucket_name}/{file_name}"

        # BigQuery load job configuration
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=True,  # Automatically detect schema
        )

        table_ref = f"{client.project}.{dataset_id}.{table_id}"
        logging.info(f"Loading data from {uri} into BigQuery table {table_ref}...")

        # Trigger the load job
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        logging.info(f"Data successfully loaded into BigQuery table {table_ref}.")
    except Exception as e:
        logging.error(f"Error loading data into BigQuery: {e}")
        raise

# Example usage
if __name__ == "__main__":
    load_to_bigquery(
        bucket_name="car-sales-data",
        file_name="enriched_dataset.csv",
        dataset_id="vehicle_analysis",
        table_id="enriched_data"
    )
