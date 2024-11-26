import pandas as pd

# Path to your CSV file
csv_file = 'Base_data.csv'

# Load the CSV file into a DataFrame
df = pd.read_csv(csv_file)

# Specify the column you want to check for duplicates
column_name = 'VIN'

# Check for duplicates
duplicates = df[df.duplicated(subset=column_name, keep=False)]

if not duplicates.empty:
    print(f"Found duplicates in column '{column_name}':")
    print(duplicates)
else:
    print(f"No duplicates found in column '{column_name}'.")

# Optionally save the duplicate rows to a new CSV file
output_file = 'duplicates.csv'
duplicates.to_csv(output_file, index=False)
print(f"Duplicate rows saved to {output_file}")
