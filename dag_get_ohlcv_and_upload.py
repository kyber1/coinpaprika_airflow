from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import requests
import json
from google.cloud import storage

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 2, tzinfo=timezone.utc),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'fetch_and_upload_ohlcv_data',
    default_args=default_args,
    description='Fetch OHLCV data and upload to Google Cloud Storage',
    schedule_interval='55 10,11 * * *',
    catchup=False,
)

# API base URL
base_url = "https://api.coinpaprika.com/v1"

def get_ohlcv_latest_data(coin_id):
    full_url = f"{base_url}/coins/{coin_id}/ohlcv/latest"
    try:
        response = requests.get(full_url)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 402:
            print(f"Payment Required for {coin_id}: {response.text}")
            return None
        elif response.status_code == 404:
            print(f"Not Found: {coin_id} might be invalid or unsupported.")
            return None
        else:
            print(f"Error {response.status_code} for {coin_id}: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Network error for {coin_id}: {e}")
        return None

def process_and_upload_coin_batch():
    current_time = datetime.now(timezone.utc)
    print(f'Current time is: {current_time}')
    if current_time.hour == 17:
        batch_number = 1
    elif current_time.hour == 11:
        batch_number = 2
    else:
        print("No batch to process at this time.")
        return

    with open('/opt/airflow/dags/coinpaprika_airflow/api_ids.txt', 'r') as f:
        api_ids = [line.strip() for line in f if line.strip()]

    batch_size = 50
    batches = [api_ids[i:i + batch_size] for i in range(0, len(api_ids), batch_size)]

    coins_ids = batches[batch_number - 1]

    all_ohlcv_data = {}
    for coin_id in coins_ids:
        ohlcv_data = get_ohlcv_latest_data(coin_id)
        if ohlcv_data:
            all_ohlcv_data[coin_id] = ohlcv_data[0]
            print(f"Fetched OHLCV data for {coin_id}")
        else:
            print(f"Skipped {coin_id} due to errors.")

    if not all_ohlcv_data:
        print("No data fetched to upload.")
        return

    # Convert data to JSON string
    data_json = json.dumps(all_ohlcv_data, indent=4)

    # Prepare GCS upload
    project_id = 'tough-bearing-436219-r4'
    bucket_name = 'coinpaprika_bronze'

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    # Create a blob name with date folders
    now = datetime.now(timezone.utc)
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')

    blob_name = f"{year}/{month}/{day}/all_ohlcv_latest_batch_{batch_number}.json"

    blob = bucket.blob(blob_name)
    blob.upload_from_string(data_json, content_type='application/json')
    print(f"Uploaded data to bucket {bucket_name} with blob name {blob_name}")

process_and_upload_task = PythonOperator(
    task_id='process_and_upload_task',
    python_callable=process_and_upload_coin_batch,
    provide_context=True,
    dag=dag,
)
