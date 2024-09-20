from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os

# Define the path to save data temporarily
TEMP_DATA_FILE = '/usr/local/airflow/data/posts.json'  # Path inside the container

def fetch_data():
    url = 'https://jsonplaceholder.typicode.com/posts'  # Example API
    response = requests.get(url)
    data = response.json()

    # Write the data to a local file
    with open(TEMP_DATA_FILE, 'w') as f:
        json.dump(data, f)
    print(f'Data fetched and saved to {TEMP_DATA_FILE}')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(dag_id='fetch_data_to_local_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data,
    )
