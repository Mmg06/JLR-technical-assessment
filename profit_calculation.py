from google.cloud import storage
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
        # Use StringIO from the 'io' module to handle in-memory text stream
        df = pd.read_csv(StringIO(data))  
        logging.info(f"Successfully read {file_name} from bucket {bucket_name}")
        return df
    except Exception as e:
        logging.error(f"Error reading {file_name} from GCS: {e}")
        raise

# Function to extract the model code 
def extract_model_code(df):
    # Extracts the model code that follows the pattern: Alphabet + 3 digits (e.g., L320, X152)
    df['Model_Code'] = df['Model_Text'].str.extract(r'([A-Z]\d{3})', expand=False)

    df['Model_Code'].fillna(df['Model_Text'], inplace=True)

    return df

# Function to calculate the average material cost by Options_Code
def calculate_avg_material_cost(options_df):
    return options_df.groupby('Options_Code')['Material_Cost'].mean().reset_index()

# Function to enrich the dataset based on the given logic
def enrich_dataset(base_df, options_df):
    try:
        # Extract Model_Code without changing Model_Text
        base_df = extract_model_code(base_df)

        # Step 1: Set production_cost to 0 if Sales_Price is zero or negative
        base_df['production_cost'] = 0
        base_df.loc[base_df['Sales_Price'] <= 0, 'production_cost'] = 0

        # Step 2: Merge with options dataset to get material cost where there is an exact match
        merged_df = base_df.merge(
            options_df[['Options_Code', 'Model', 'Material_Cost']], 
            left_on=['Options_Code', 'Model_Code'], 
            right_on=['Options_Code', 'Model'], 
            how='left'  # Perform the left join, will result in NaN for unmatched rows
        )

        # Step 3: Calculate the average material cost per option code
        avg_material_cost = calculate_avg_material_cost(options_df)

        # Step 4: For records where there is no exact match or Model is null, use the average Material_Cost
        merged_df = merged_df.merge(
            avg_material_cost, 
            on='Options_Code', 
            how='left', 
            suffixes=('', '_avg')
        )

        # # Step 5: If Material_Cost is NaN (no exact match or Model is null), use the average Material_Cost
        # merged_df['production_cost'] = merged_df['Material_Cost'].combine_first(merged_df['Material_Cost_avg'])

        # Step 5: For records still without a value for production_cost, use 45% of Sales_Price
        merged_df['production_cost'].fillna(merged_df['Sales_Price'] * 0.45, inplace=True)

        # Step 6: Ensure that production cost is zero where Sales_Price is zero or negative
        merged_df.loc[merged_df['Sales_Price'] <= 0, 'production_cost'] = 0

        # Step 7: Calculate profit (Sales_Price - production_cost)
        merged_df['profit'] = merged_df['Sales_Price'] - merged_df['production_cost']

        # Step 8: Remove rows where 'Sales_Price' is NaN
        merged_df = merged_df.dropna(subset=['Sales_Price'])

        # Step 9: Drop unwanted columns (Model, Material_Cost, and any duplicates)
        merged_df = merged_df.drop(columns=['Model', 'Material_Cost_avg', 'Material_Cost'])


        logging.info("Dataset successfully enriched and null values removed")
        return merged_df
    except Exception as e:
        logging.error(f"Error enriching dataset: {e}")
        raise

# Main function to execute the workflow and generate CSV
def main():
    bucket_name = 'vehicle-inventory'
    base_file_name = 'Base_data.csv.csv'
    options_file_name = 'options_data.csv'
    
    try:
        # Read data from GCS
        base_df = read_data_from_gcs(bucket_name, base_file_name)
        options_df = read_data_from_gcs(bucket_name, options_file_name)

        # Enrich the dataset
        enriched_df = enrich_dataset(base_df, options_df)

        # Save the enriched DataFrame to a CSV file
        output_csv = "enriched_dataset.csv"
        enriched_df.to_csv(output_csv, index=False)
        logging.info(f"Enriched dataset has been saved to {output_csv}")
        print(f"Enriched dataset has been saved to {output_csv}")

    except Exception as e:
        logging.error(f"Error in the workflow: {e}")

if __name__ == "__main__":
    main()
