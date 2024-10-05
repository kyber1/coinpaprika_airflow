from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
import requests
from datetime import datetime

project_id = 'tough-bearing-436219-r4'
bucket_name = 'coinpaprika_bronze'

def fetch_and_upload(**kwargs):
    url = 'https://jsonplaceholder.typicode.com/posts'
    response = requests.get(url)
    data = response.text

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("test_api_response.text")
    blob.upload_from_string(data)

default_args = {
    'start_date': datetime(2024, 10, 4),
    'catchup': False
}

dag = DAG(
    'fetch_and_upload_data',
    default_args=default_args,
    description='Fetch data from API and upload to Google Cloud Storage',
    schedule_interval=None,
    catchup=False
)

fetch_and_upload_task = PythonOperator(
    task_id='fetch_and_upload_task',
    python_callable=fetch_and_upload,
    provide_context=True,
    dag=dag,
)
