import requests
import os
import json
from datetime import datetime, timedelta
import time

def news_fetch():
    today = datetime.now()
    start = today - timedelta(days=30)

    # Specify the path for the JSON file
    json_path = os.path.join("data", "raw", "news.json")

    # Check if the file exists, if not, create it
    if not os.path.exists(json_path):
        with open(json_path, "w") as file:
            file.write("[]")

    # Load existing JSON data
    with open(json_path, "r") as file:
        existing_data = json.load(file)

    while start <= today:
        # Make the API request
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": "metaverse",
            "apiKey": "84070c6bdc904a6abbbd7a575b346a7c",
            "from": start.strftime("%Y-%m-%d"),
            "to": (start + timedelta(days=2)).strftime("%Y-%m-%d"),
            "language": "en",
        }

        response = requests.get(url, params=params)
        data = response.json()
        articles = data["articles"]

        # Process the articles and create a list of dictionaries
        news_data = []
        for article in articles:
            title = article["title"]
            description = article["description"]
            content = article["content"]
            publish_date = article["publishedAt"]
            news_item = {
                "title": title,
                "description": description,
                "content": content,
                "publish_date": publish_date
            }
            news_data.append(news_item)

        # Append the new data to the existing JSON data
        existing_data.extend(news_data)

        # Save the updated JSON data to the file
        with open(json_path, "w") as file:
            json.dump(existing_data, file, indent=4)

        print(f"Fetched {len(articles)} articles from {start.strftime('%Y-%m-%d')} to {(start + timedelta(days=2)).strftime('%Y-%m-%d')}")
        time.sleep(30)  # Sleep for 30 seconds before making the next request
        start += timedelta(days=2)

    print(f"JSON file saved at: {json_path}")
    print(f"Total number of articles fetched: {len(existing_data)}")

