from google.cloud import storage
import pandas as pd
import logging
from io import StringIO  
import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Configure logging
logging.basicConfig(level=logging.INFO)

# Function to read data from GCS
def read_data_from_gcs(bucket_name, file_name):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        data = blob.download_as_text()
        # Use StringIO from the 'io' module to handle in-memory text stream
        df = pd.read_csv(StringIO(data))  
        logging.info(f"Successfully read {file_name} from bucket {bucket_name}")
        return df
    except Exception as e:
        logging.error(f"Error reading {file_name} from GCS: {e}")
        raise

def clean_vin_column(df):
    df['VIN'] = df['VIN'].str.strip().str.lower()
    clean_df = df[(~df['VIN'].isna()) & (df['VIN'] != 'unknown')]
    return clean_df

# Function to extract the model code 
def extract_model_code(df):
    # Extract a valid pattern ([A-Z]+ followed by \d{3}) or keep the entire Model_Text if no match
    df['Model_Code'] = df['Model_Text'].str.extract(r'([A-Z]+[0-9]{3})', expand=False)
    df['Model_Code'] = df['Model_Code'].fillna(df['Model_Text'])  # Fill with original text if no match
    print("Extracted Model_Code:\n", df[['Model_Text', 'Model_Code']])
    return df

# Function to calculate the average material cost by Options_Code
def calculate_avg_material_cost(options_df):
    return options_df.groupby('Options_Code')['Material_Cost'].mean().reset_index()
def enrich_dataset(base_df, options_df):
    try:
        # Extract Model_Code without changing Model_Text
        base_df = extract_model_code(base_df)

        # Deduplicate options_df to avoid duplicate rows during merging
        options_df = options_df.drop_duplicates(subset=['Options_Code', 'Model'])

        # Merge with options dataset to get material cost where there is an exact match
        merged_df = base_df.merge(
            options_df[['Options_Code', 'Model', 'Material_Cost']],
            left_on=['Options_Code', 'Model_Code'],
            right_on=['Options_Code', 'Model'],
            how='left'
        )

        # Calculate the average material cost per option code
        avg_material_cost = calculate_avg_material_cost(options_df)

        # Add average material cost for missing values
        merged_df = merged_df.merge(
            avg_material_cost,
            on='Options_Code',
            how='left',
            suffixes=('', '_avg')
        )

        # Fill production_cost with Material_Cost or 45% of Sales_Price
        merged_df['production_cost'] = merged_df['Material_Cost'].fillna(
            merged_df['Sales_Price'] * 0.45
        )
        # Ensure production_cost is 0 where Sales_Price <= 0
        merged_df.loc[merged_df['Sales_Price'] <= 0, 'production_cost'] = 0

        # Calculate profit
        merged_df['profit'] = merged_df['Sales_Price'] - merged_df['production_cost']

        # Remove rows where Sales_Price <= 0
        merged_df = merged_df[merged_df['Sales_Price'] > 0]

        # Drop unwanted columns
        merged_df = merged_df.drop(columns=['Model', 'Material_Cost', 'Material_Cost_avg'])

        logging.info("Dataset successfully enriched and null values removed")
        return merged_df
    except Exception as e:
        logging.error(f"Error enriching dataset: {e}")
        raise

# # Function to enrich the dataset based on the given logic
# def enrich_dataset(base_df, options_df):
#     try:
#         # Extract Model_Code without changing Model_Text
#         base_df = extract_model_code(base_df)

#         # Step 1: Set production_cost to 0 if Sales_Price is zero or negative
#         base_df['production_cost'] = 0
#         base_df.loc[base_df['Sales_Price'] <= 0, 'production_cost'] = 0

#         # Step 2: Merge with options dataset to get material cost where there is an exact match
#         merged_df = base_df.merge(
#             options_df[['Options_Code', 'Model', 'Material_Cost']], 
#             left_on=['Options_Code', 'Model_Code'], 
#             right_on=['Options_Code', 'Model'], 
#             how='left' 
#         )

#         # Step 3: Calculate the average material cost per option code
#         avg_material_cost = calculate_avg_material_cost(options_df)

#         # Step 4: For records where there is no exact match or Model is null, use the average Material_Cost
#         merged_df = merged_df.merge(
#             avg_material_cost, 
#             on='Options_Code', 
#             how='left', 
#             suffixes=('', '_avg')
#         )

#         # Step 5: For records still without a value for production_cost, use 45% of Sales_Price
#         merged_df['production_cost'].fillna(merged_df['Sales_Price'] * 0.45, inplace=True)

#         # Step 6: Ensure that production cost is zero where Sales_Price is zero or negative
#         merged_df.loc[merged_df['Sales_Price'] <= 0, 'production_cost'] = 0

#         # Step 7: Calculate profit (Sales_Price - production_cost)
#         merged_df['profit'] = merged_df['Sales_Price'] - merged_df['production_cost']

#         # Step 8: Remove rows where 'Sales_Price' is NaN
#         merged_df = merged_df.dropna(subset=['Sales_Price'])

#         # Step 9: Drop unwanted columns (Model, Material_Cost, and any duplicates)
#         merged_df = merged_df.drop(columns=['Model', 'Material_Cost_avg', 'Material_Cost'])

#         logging.info("Dataset successfully enriched and null values removed")
#         return merged_df
#     except Exception as e:
#         logging.error(f"Error enriching dataset: {e}")
#         raise

def upload_to_gcs(bucket_name, destination_blob_name, dataframe):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # Convert the DataFrame to CSV in-memory
        csv_data = dataframe.to_csv(index=False)
        
        # Upload the CSV data
        blob.upload_from_string(csv_data, content_type="text/csv")
        logging.info(f"Enriched data uploaded to {bucket_name}/{destination_blob_name}")
    except Exception as e:
        logging.error(f"Error uploading enriched data to GCS: {e}")
        raise

def main():
    bucket_name = 'car-sales-data'
    base_file_name = 'base_data.csv'
    options_file_name = 'options_data.csv'
    enriched_file_name = 'enriched_dataset.csv'  # Destination file name in GCS
    
    try:
        # Step 1: Read data from GCS
        base_df = read_data_from_gcs(bucket_name, base_file_name)
        options_df = read_data_from_gcs(bucket_name, options_file_name)

        # Step 2: Clean the VIN column
        base_df = clean_vin_column(base_df)
        logging.info("VIN column cleaned")
        
        # Step 3: Check and remove duplicates
        if base_df.duplicated().any():
            original_size = len(base_df)
            base_df = base_df.drop_duplicates()
            logging.info(f"Removed {original_size - len(base_df)} duplicates from the dataset.")
        else:
            logging.info("No duplicates found in the dataset.")

        # Check if base_df is empty after cleaning and deduplication, to handle cases where all data might be filtered out
        if base_df.empty:
            logging.error("All data was removed after cleaning and removing duplicates from the VIN column. No data left to process.")
            return  # Exit if no data is available for further processing

        # Step 4: Enrich the dataset using the cleaned DataFrame
        enriched_df = enrich_dataset(base_df, options_df)

        # Step 5: Save the enriched DataFrame locally 
        local_csv = "enriched_dataset_local.csv"
        enriched_df.to_csv(local_csv, index=False)
        logging.info(f"Enriched dataset saved locally to {local_csv}")

        # Step 6: Upload the enriched dataset to GCS
        upload_to_gcs(bucket_name, enriched_file_name, enriched_df)
        logging.info(f"Enriched dataset successfully uploaded to GCS as {enriched_file_name}")
        print(f"Enriched dataset has been uploaded to GCS as {enriched_file_name}")

    except Exception as e:
        logging.error(f"Error in the workflow: {e}")

if __name__ == "__main__":
    main()

