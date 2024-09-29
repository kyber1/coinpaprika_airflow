from google.cloud import storage
import requests

project_id = 'tough-bearing-436219-r4' 
bucket_name = 'coinpaprika_bronze'

def fetch_data():
    url = 'https://jsonplaceholder.typicode.com/posts'  # Example API
    response = requests.get(url)
    return response.text


def upload_blob(data):
    #connect to google cloud 
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("test_api_response.text")

    blob.upload_from_string(data)

data = fetch_data()
upload_blob(data)