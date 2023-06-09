import os,csv
from datetime import date
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError

parent_dir = os.path.dirname(os.getcwd())
processed_stock_path = os.path.join(parent_dir, "big_data_project", "data", "processed", "processed_stock_data.csv")
processed_news_path = os.path.join(parent_dir, "big_data_project", "data", "processed", "processed_news_data.csv")

def csv_to_list_of_dicts(csv_path):
    result = []
    with open(csv_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            result.append(row)
    return result


def index_data(path,index_name):
    CLOUD_ID = 'b9299417ffa74630a2ae132fabd665e5:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ0OTYwZWZjZjMwYTQ0YzI1YjM3NjVkZTVhOWZhNzY3NCQ1ODY3OTdmNTdlZWU0ZjQ5OGYzODkwMTMyNjViMmMwOA=='
    ELASTIC_PASSWORD = 'oHPPKIuBpqM4X9dLG0HEiOfO'
    # Connect to Elasticsearch
    es = Elasticsearch(cloud_id=CLOUD_ID, basic_auth=("elastic", ELASTIC_PASSWORD))
    
    # Specify the index name
    dict = csv_to_list_of_dicts(path) 
    fields = list(dict[0].keys())

     # Define the index mapping
    index_mapping = {
        "mappings": {
            "properties": {
                fields[0]: {
                    "type": "date"
                },
                fields[1]: {
                    "type": "float"
                }
            }
        }
    }

    # Create the index with explicit mapping
    es.indices.create(index=index_name, body=index_mapping)

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


# index_data(processed_stock_path,"prices_daily_avg")
# index_data(processed_news_path,"news_scores_avg")
# Get the list of indexes
CLOUD_ID = 'b9299417ffa74630a2ae132fabd665e5:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ0OTYwZWZjZjMwYTQ0YzI1YjM3NjVkZTVhOWZhNzY3NCQ1ODY3OTdmNTdlZWU0ZjQ5OGYzODkwMTMyNjViMmMwOA=='
ELASTIC_PASSWORD = 'oHPPKIuBpqM4X9dLG0HEiOfO'
# Connect to Elasticsearch
es = Elasticsearch(cloud_id=CLOUD_ID, basic_auth=("elastic", ELASTIC_PASSWORD))
# Specify the index name
index_name = "prices_daily_avg"

# Search for all documents in the index
query = {
    "query": {
        "match_all": {}
    },
    "size": 1000  # Set the size parameter to retrieve all documents (adjust the value as needed)
}

# Execute the search query
response = es.search(index=index_name, body=query)

# Print the documents
for hit in response["hits"]["hits"]:
    source = hit["_source"]
    print(source)