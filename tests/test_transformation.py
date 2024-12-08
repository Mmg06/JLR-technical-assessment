import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_transformation import extract_model_code, calculate_avg_material_cost, enrich_dataset
import unittest
import pandas as pd
from io import StringIO


class TestTransformationScript(unittest.TestCase):

    def test_extract_model_code(self):
        # Sample input DataFrame
        data = {'Model_Text': ['L320-X', 'X152-Y', 'ABC123']}
        df = pd.DataFrame(data)

        # Expected output
        expected_data = {
            'Model_Text': ['L320-X', 'X152-Y', 'ABC123'],
            'Model_Code': ['L320', 'X152', 'ABC123']
        }
        expected_df = pd.DataFrame(expected_data)

        # Test the function
        result_df = extract_model_code(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_calculate_avg_material_cost(self):
        # Sample options DataFrame
        data = {
            'Options_Code': ['A', 'A', 'B', 'C', 'C'],
            'Material_Cost': [100, 200, 300, 400, 500]
        }
        options_df = pd.DataFrame(data)

        # Expected average cost per Options_Code
        expected_data = {
            'Options_Code': ['A', 'B', 'C'],
            'Material_Cost': [150.0, 300.0, 450.0]
        }
        expected_df = pd.DataFrame(expected_data)

        # Test the function
        result_df = calculate_avg_material_cost(options_df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_enrich_dataset(self):
        # Sample base DataFrame
        base_data = {
            'Options_Code': ['A', 'B', 'C'],
            'Model_Text': ['L320', 'X152', 'XYZ'],
            'Sales_Price': [1000, -500, 200],
        }
        base_df = pd.DataFrame(base_data)

        # Sample options DataFrame
        options_data = {
            'Options_Code': ['A', 'A', 'B'],
            'Model': ['L320', 'L320', 'X152'],
            'Material_Cost': [300, 400, 200],
        }
        options_df = pd.DataFrame(options_data)

        # Expected output
        expected_data = {
            'Options_Code': ['A', 'C'],  # Exclude 'B' due to Sales_Price <= 0
            'Model_Text': ['L320', 'XYZ'],
            'Sales_Price': [1000, 200],
            'Model_Code': ['L320', 'XYZ'],
            'production_cost': [300, 90],  # Keep this as int
            'profit': [700, 110],  # Profit is int
        }
        expected_df = pd.DataFrame(expected_data)

        # Ensure the expected production_cost and profit columns are explicitly int64
        expected_df['production_cost'] = expected_df['production_cost'].astype('int64')
        expected_df['profit'] = expected_df['profit'].astype('int64')

        # Test the function
        result_df = enrich_dataset(base_df, options_df)

        # Reset the index of the result DataFrame
        result_df = result_df.reset_index(drop=True)

        # Cast production_cost and profit to int64 for comparison
        result_df['production_cost'] = result_df['production_cost'].astype('int64')
        result_df['profit'] = result_df['profit'].astype('int64')

        # Debug output
        print("Result DataFrame from enrich_dataset (after reset_index and casting):\n", result_df)

        pd.testing.assert_frame_equal(
            result_df[['Options_Code', 'Model_Text', 'Sales_Price', 'Model_Code', 'production_cost', 'profit']],
            expected_df
        )



if __name__ == '__main__':
    unittest.main()

