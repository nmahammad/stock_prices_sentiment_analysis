import os
import json
import yfinance as yf
from datetime import datetime, timedelta

def yahoo_fetch():
    today = datetime.now()
    seven_days_ago = today - timedelta(days=7)

    metv = yf.download(
        tickers="METV",
        start=seven_days_ago,
        end=today,
        period="1d",
        interval="1m"
    )

    print(metv)
    
    # Convert dataframe to JSON
    metv_json = metv.to_json(orient="records")

    # Specify the path for the JSON file
    json_path = os.path.join("data", "raw", "metv.json")

    # Check if the file exists, if not, create it
    if not os.path.exists(json_path):
        with open(json_path, "w") as file:
            file.write("[]")

    # Load existing JSON data
    with open(json_path, "r") as file:
        existing_data = json.load(file)

    # Append the new data to the existing JSON data
    existing_data.extend(json.loads(metv_json))

    # Save the updated JSON data to the file
    with open(json_path, "w") as file:
        json.dump(existing_data, file, indent=4)

    print(f"JSON file saved at: {json_path}")
    
    return metv
