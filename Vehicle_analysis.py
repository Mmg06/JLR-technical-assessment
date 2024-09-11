from google.cloud import storage, bigquery
import pandas as pd
import logging
from io import StringIO 

# Configure logging
logging.basicConfig(level=logging.INFO)

# Function to read data from GCS
def read_data_from_gcs(bucket_name, file_name):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        data = blob.download_as_text()
        df = pd.read_csv(StringIO(data)) 
        logging.info(f"Successfully read {file_name} from bucket {bucket_name}")
        return df
    except Exception as e:
        logging.error(f"Error reading {file_name} from GCS: {e}")
        raise

# Function to extract the model code 
def extract_model_code(df):
    df['Model_Code'] = df['Model_Text'].str.extract(r'([A-Z]\d{3})', expand=False)
    df['Model_Code'].fillna(df['Model_Text'], inplace=True)
    return df

# Function to calculate the average material cost by Options_Code
def calculate_avg_material_cost(options_df):
    return options_df.groupby('Options_Code')['Material_Cost'].mean().reset_index()

# Function to enrich the dataset based on the given logic
def enrich_dataset(base_df, options_df):
    try:
        base_df = extract_model_code(base_df)
        base_df['production_cost'] = 0
        base_df.loc[base_df['Sales_Price'] <= 0, 'production_cost'] = 0 

        merged_df = base_df.merge(
            options_df[['Options_Code', 'Model', 'Material_Cost']],
            left_on=['Options_Code', 'Model_Code'],
            right_on=['Options_Code', 'Model'],
            how='left'
        )

        avg_material_cost = calculate_avg_material_cost(options_df)
        merged_df = merged_df.merge(
            avg_material_cost,
            on='Options_Code',
            how='left',
            suffixes=('', '_avg')
        )

        merged_df['production_cost'] = merged_df['Material_Cost'].combine_first(merged_df['Material_Cost_avg'])
        merged_df['production_cost'].fillna(merged_df['Sales_Price'] * 0.45, inplace=True)
        merged_df.loc[merged_df['Sales_Price'] <= 0, 'production_cost'] = 0

        merged_df['profit'] = merged_df['Sales_Price'] - merged_df['production_cost']
        merged_df = merged_df.drop(columns=['Model', 'Material_Cost_avg', 'Material_Cost'])
        merged_df = merged_df.dropna(subset=['Sales_Price'])

        logging.info("Dataset successfully enriched and null values removed")
        return merged_df
    except Exception as e:
        logging.error(f"Error enriching dataset: {e}")
        raise

# Function to save the enriched DataFrame to BigQuery
def save_to_bigquery(df, project_id, dataset_id, table_id):
    try:
        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  
        )

        load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        logging.info(f"Successfully loaded data into BigQuery table: {dataset_id}.{table_id}")
    except Exception as e:
        logging.error(f"Error loading data into BigQuery: {e}")
        raise

# Main function to execute the workflow and load data into BigQuery
def main():
    bucket_name = 'vehicle-inventory'
    base_file_name = 'Base_data.csv.csv'
    options_file_name = 'options_data.csv'
    project_id = 'jlr-interview-435215'
    dataset_id = 'vehicle_analysis'
    table_id = 'summary_data'  

    try:
        # Read data from GCS
        base_df = read_data_from_gcs(bucket_name, base_file_name)
        options_df = read_data_from_gcs(bucket_name, options_file_name)

        # Enrich the dataset
        enriched_df = enrich_dataset(base_df, options_df)

        # Save the enriched DataFrame to BigQuery
        save_to_bigquery(enriched_df, project_id, dataset_id, table_id)

    except Exception as e:
        logging.error(f"Error in the workflow: {e}")

if __name__ == "__main__":
    main()
