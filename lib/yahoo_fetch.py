import os
import json
import yfinance as yf
from datetime import datetime, timedelta

def convert_timestamp_to_datetime(timestamp):
    # Convert milliseconds to seconds
    timestamp_seconds = timestamp // 1000

    # Convert timestamp to datetime object
    datetime_obj = datetime.fromtimestamp(timestamp_seconds)

    # Convert datetime object to desired date format
    formatted_date = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    return formatted_date



def yahoo_fetch_yearly():
    today = datetime.now()
    start_date = today - timedelta(days=2 * 365)

    metv = yf.download(
        tickers="METV",
        start=start_date,
        end=today,
        interval="1d"
    )

    # Convert dataframe to JSON with custom date format
    metv_json = metv.reset_index().to_json(orient="records", date_format="iso")

    # Specify the path for the JSON file
    json_path = os.path.join("data", "raw", "metv_yearly.json")

    # Check if the file exists, if not, create it
    if not os.path.exists(json_path):
        with open(json_path, "w") as file:
            file.write("[]")

    # Load existing JSON data
    with open(json_path, "r") as file:
        existing_data = json.load(file)

    # Convert the timestamp to desired date format before adding it to the JSON
    for item in existing_data:
        if "Datetime" in item:
            timestamp = item["Datetime"]
            formatted_date = convert_timestamp_to_datetime(timestamp)
            item["Datetime"] = formatted_date

    # Append the new data to the existing JSON data
    existing_data.extend(json.loads(metv_json))

    # Save the updated JSON data to the file
    with open(json_path, "w") as file:
        json.dump(existing_data, file, indent=4)

    print(f"JSON file saved at: {json_path}")

    return metv


def yahoo_fetch_monthly():
    today = datetime.now()
    start_date = today - timedelta(days=30)

    metv = yf.download(
        tickers="METV",
        start=start_date,
        end=today,
        interval="1d"
    )

    # Convert dataframe to JSON with custom date format
    metv_json = metv.reset_index().to_json(orient="records", date_format="iso")

    # Specify the path for the JSON file
    json_path = os.path.join("data", "raw", "metv_montly.json")

    # Check if the file exists, if not, create it
    if not os.path.exists(json_path):
        with open(json_path, "w") as file:
            file.write("[]")

    # Load existing JSON data
    with open(json_path, "r") as file:
        existing_data = json.load(file)

    # Convert the timestamp to desired date format before adding it to the JSON
    for item in existing_data:
        if "Datetime" in item:
            timestamp = item["Datetime"]
            formatted_date = convert_timestamp_to_datetime(timestamp)
            item["Datetime"] = formatted_date

    # Append the new data to the existing JSON data
    existing_data.extend(json.loads(metv_json))

    # Save the updated JSON data to the file
    with open(json_path, "w") as file:
        json.dump(existing_data, file, indent=4)

    print(f"JSON file saved at: {json_path}")

    return metv


yahoo_fetch_monthly()