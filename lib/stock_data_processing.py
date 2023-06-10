import os
import pandas as pd

# Getting the correct path for files
current_file_path = os.path.abspath(__file__)
parent_folder_path = os.path.dirname(os.path.dirname(current_file_path))
data_folder_path = os.path.join(parent_folder_path, "data", "raw")
output_directory = os.path.join(parent_folder_path, "data", "processed")


def process_data(input_file, output_file):
    # Read the JSON file using pandas
    df = pd.read_json(input_file)

    # Select only the desired columns
    selected_df = df[["Date", "Adj Close", "Volume"]]

    # Save the DataFrame as a CSV file
    selected_df.to_csv(output_file, index=False)
    print(f"Data saved successfully: {output_file}")


# # Process yearly data
# yearly_data_path = os.path.join(data_folder_path, "metv_yearly.json")
# yearly_data_output = os.path.join(output_directory, "processed_yearly_data.csv")
# process_data(yearly_data_path, yearly_data_output)

# Process monthly data
monthly_data_path = os.path.join(data_folder_path, "metv_monthly.json")
monthly_data_output = os.path.join(output_directory, "processed_monthly_data.csv")
process_data(monthly_data_path, monthly_data_output)
