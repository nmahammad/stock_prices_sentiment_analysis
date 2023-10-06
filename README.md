# Project Overview 📈

**Welcome to the Stock Price and News Sentiment Analysis for the Metaverse project!** I undertook this project for educational purposes as part of my curriculum.

🚀 Here, I explore the intriguing **correlation between stock price movements and changes in sentiment** related to the "metaverse." My goal is to provide a valuable tool for investors who seek to make informed decisions in the stock market.

🧠 I have used a model that leverages the power of **Natural Language Processing (NLP)** and **Machine Learning (ML)** to analyze news sentiment and make predictions about future stock market trends.

My approach involves analyzing **news articles related to the metaverse** to determine their sentiment. By combining this sentiment analysis with other relevant indicators, I aim to predict trends in stock price movements. Investors can use these predictions to guide their investment decisions.

## Data Processing 📊

I utilize **Python Pandas, Spark, and Airflow dags** to process and manage my data efficiently.

## Architecture Overview 🏢

### File Structure 📂

Before diving into the specifics of my Airflow DAG structure, let's take a moment to discuss the organization of my project files. Proper file organization is crucial for efficient development and maintenance.

My file structure includes:

- **DAGs Folder:** Contains a single DAG file that defines the tasks for my data processing pipeline.
  
- **Lib Folder:** This directory contains files responsible for fetching and processing data.

- **Data Folder:** My data lake, consisting of two subfolders:
  - **Raw:** Where I initially store the raw data collected.
  - **Processed:** After data processing, the resulting files are moved to this folder.

My project architecture aims to streamline data collection, processing, and analysis, enabling me to deliver accurate and timely insights into the stock market trends.

<img src="dags structure.png" alt="Logo">

Thank you for exploring my Stock Price and News Sentiment Analysis for the Metaverse project. I hope you find it informative and educational. Feel free to reach out with any questions or feedback! 🚀📊
