from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
import os
import pandas as pd
from datetime import datetime, timedelta

parent_dir = os.path.dirname(os.getcwd())
metv_path = os.path.join(parent_dir, "data", "raw", "metv_monthly.json")

df_metv = pd.read_json(metv_path)
df_metv['Date'] = pd.to_datetime(df_metv['Date'])

news_path = os.path.join(parent_dir, "data", "raw", "news.json")
df_news = pd.read_json(news_path)
df_news['publish_date'] = pd.to_datetime(df_news['publish_date'])
df_news['Date'] = df_news['publish_date'].dt.strftime('%Y-%m-%d')

# Convert the 'Date' column in both dataframes to datetime
df_news['Date'] = pd.to_datetime(df_news['Date'])
df_metv['Date'] = pd.to_datetime(df_metv['Date'])

combined_df = pd.merge(df_news, df_metv, on='Date', how='outer')

# Sort the dataframe by 'Date' column
combined_df = combined_df.sort_values('Date')

# Reset the index of the combined dataframe
combined_df = combined_df.reset_index(drop=True)
combined_df.dropna(inplace=True)

#Create the final text for implementing NLP
combined_df["summary"] = combined_df["title"] + " " + combined_df["description"] + " " + combined_df["content"]
import flair

# Load the sentiment classifier
classifier = flair.models.TextClassifier.load('en-sentiment')

# Define a function for sentiment analysis
def sentiment_analysis(text):
    sentence = flair.data.Sentence(text)
    classifier.predict(sentence)
    label = sentence.labels[0].value
    score = sentence.labels[0].score
    return label, score

data_dict = df_news['summary'].to_dict()
# Apply sentiment analysis to the dictionary
sentiment_results = {key: sentiment_analysis(value) for key, value in data_dict.items()}
# Convert sentiment results dictionary to DataFrame
sentiment_df = pd.DataFrame.from_dict(sentiment_results, orient='index', columns=['label', 'score'])

# Merge sentiment results DataFrame with the original DataFrame
df_news = pd.concat([df_news, sentiment_df], axis=1)

columns_to_drop = ["title", "description", "content", "publish_date", "Open", "High", "Low", "Close", "summary"]
df_news.drop(columns=columns_to_drop, inplace=True)
df_news['score'] = df_news.apply(lambda row: row['score'] * -1 if row['label'] == 'NEGATIVE' else row['score'], axis=1)
df_news.head()

# Calculate average score for each date in df_news
df_news_avg_scores = df_news.groupby('Date')['score'].mean().reset_index()

# Merge df_news_avg_scores with df_metv based on 'Date'
merged_df = df_news_avg_scores.merge(df_metv[['Date', 'Adj Close', 'Volume']], on='Date', how='inner')

# Display the resulting DataFrame with selected columns
result_df = merged_df[['Date', 'Adj Close', 'Volume', 'score']]
result_df['score'] = result_df['score'] + 10

current_file_path = os.getcwd()
parent_folder_path = os.path.dirname(os.path.dirname(current_file_path))
output_directory = os.path.join(parent_folder_path, 'big_data_project', "data", "processed")

monthly_data_output = os.path.join(output_directory, "processed_monthly_data.csv")
result_df.to_csv(monthly_data_output, index=False)