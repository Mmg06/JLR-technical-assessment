import pandas as pd

# Load the datasets
base_data_path = 'C:/desktop/JLR_Technical_Interview/Base_data.csv.csv'  # Adjust path as needed
options_data_path = 'C:/desktop/JLR_Technical_Interview/options_data.csv'

# Load the file as a CSV instead of Excel
base_df = pd.read_csv(base_data_path)  
options_df = pd.read_csv(options_data_path)

# Merge the base data with options data on 'Options_Code' and 'Model'
merged_df = pd.merge(base_df, options_df, left_on=['Options_Code', 'Model_Text'], right_on=['Options_Code', 'Model'], how='left')

# Update the base data with material cost from options data
base_df['Material_Cost'] = merged_df['Material_Cost']

# Show the updated base dataset
print(base_df.head())

# save the updated dataframe to a new file
base_df.to_csv('C:/desktop/JLR_Technical_Interview/Updated_base_data.csv', index=False)
