import unittest
import pandas as pd
from io import StringIO
from data_transformation import extract_model_code, calculate_avg_material_cost, enrich_dataset
import os
import sys

# Add the parent directory to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)

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
            'Options_Code': ['A', 'B', 'C'],
            'Model_Text': ['L320', 'X152', 'XYZ'],
            'Sales_Price': [1000, -500, 200],
            'Model_Code': ['L320', 'X152', 'XYZ'],
            'production_cost': [300, 0, 90],  # Material cost, zero for negative Sales_Price, 45% for no match
            'profit': [700, -500, 110],  # Sales_Price - production_cost
        }
        expected_df = pd.DataFrame(expected_data)

        # Test the function
        result_df = enrich_dataset(base_df, options_df)
        pd.testing.assert_frame_equal(
            result_df[['Options_Code', 'Model_Text', 'Sales_Price', 'Model_Code', 'production_cost', 'profit']],
            expected_df
        )

if __name__ == '__main__':
    unittest.main()
