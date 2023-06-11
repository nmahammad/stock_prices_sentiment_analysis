import os,csv
from datetime import date
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError

current_file_path = os.path.abspath(__file__)
parent_folder_path = os.path.dirname(os.path.dirname(current_file_path))

processed_stock_path = os.path.join(parent_folder_path, "data", "processed", "processed_yearly_data.csv")
processed_news_path = os.path.join(parent_folder_path, "data", "processed", "processed_monthly_data.csv")

def csv_to_list_of_dicts(csv_path):
    result = []
    with open(csv_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            result.append(row)
    return result


def index_data_news(path,index_name):
    CLOUD_ID = 'b9299417ffa74630a2ae132fabd665e5:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ0OTYwZWZjZjMwYTQ0YzI1YjM3NjVkZTVhOWZhNzY3NCQ1ODY3OTdmNTdlZWU0ZjQ5OGYzODkwMTMyNjViMmMwOA=='
    ELASTIC_PASSWORD = 'oHPPKIuBpqM4X9dLG0HEiOfO'
    # Connect to Elasticsearch
    es = Elasticsearch(cloud_id=CLOUD_ID, basic_auth=("elastic", ELASTIC_PASSWORD))
    
    # Specify the index name
    dict = csv_to_list_of_dicts(path) 
    fields = list(dict[0].keys())

    # Define the index mapping for news data
    index_mapping = {
        "mappings": {
        "properties": {
        "Date": {"type": "date"},
        "Adj Close": {"type": "float"},
        "Volume": {"type": "integer"},
        "score": {"type": "float"}
            }
        }
    }


    # Create the index with explicit mapping for news data
    es.indices.create(index=index_name,body = index_mapping)

    bulk_documents = [
    {"_index": index_name, "_source": doc} for doc in dict
    ] 

    try:
        response = bulk(es, bulk_documents)
        # Check if the indexing operation was successful
        if response[0] > 0:
            print("Indexing successful!")
        else:
            print("Indexing failed.")

    except BulkIndexError as e:
        print(f"{len(e.errors)} document(s) failed")
        for i, error in enumerate(e.errors):
            print(f"Error for document {i + 1}: {error['index']['error']}")

# index_data_news(processed_news_path,'stocks_news_correlation')






def index_data_stocks(path,index_name):
    CLOUD_ID = 'b9299417ffa74630a2ae132fabd665e5:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ0OTYwZWZjZjMwYTQ0YzI1YjM3NjVkZTVhOWZhNzY3NCQ1ODY3OTdmNTdlZWU0ZjQ5OGYzODkwMTMyNjViMmMwOA=='
    ELASTIC_PASSWORD = 'oHPPKIuBpqM4X9dLG0HEiOfO'
    # Connect to Elasticsearch
    es = Elasticsearch(cloud_id=CLOUD_ID, basic_auth=("elastic", ELASTIC_PASSWORD))
    
    # Specify the index name
    dict = csv_to_list_of_dicts(path) 

    # Define the index mapping for stock data
    index_mapping = {
        "mappings": {
            "properties": {
                "Date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                "Adj Close": {"type": "float"},
                "Volume": {"type": "integer"},
                "score": {"type": "float"}
                }
            }
        }


    # Create the index with explicit mapping
    es.indices.create(index=index_name,body = index_mapping)

    bulk_documents = [
    {"_index": index_name, "_source": doc} for doc in dict
    ] 

    try:
        response = bulk(es, bulk_documents)
        # Check if the indexing operation was successful
        if response[0] > 0:
            print("Indexing successful!")
        else:
            print("Indexing failed.")

    except BulkIndexError as e:
        print(f"{len(e.errors)} document(s) failed")
        for i, error in enumerate(e.errors):
            print(f"Error for document {i + 1}: {error['index']['error']}")

# index_data_stocks(processed_stock_path,'metaverse_stocks')
