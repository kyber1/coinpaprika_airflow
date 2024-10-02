from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import requests
import json
import os

#define arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1, tzinfo=timezone.utc),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#define DAG
dag = DAG(
    'fetch_ohlcv_data',
    default_args= default_args,
    description= 'Fetch OHLCV data for the latest full day for top 100 coins in batches',
    schedule_interval= '25 14,15 * * *',
    catchup= False,
)

#free api url
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

def get_batch_number(execution_date):
    hour = execution_date.hour
    match hour:
        case 14:
            return 1
        case 15:
            return 2
        case _:
            return None

def process_coin_batch(**context):
    execution_date = context['execution_date']
    batch_number = get_batch_number(execution_date)
    
    if batch_number is None:
        print("No batch to process at this time.")
        return

    with open('/opt/airflow/dags/coinpaprika_airflow/api_ids.txt', 'r') as f:
        api_ids = [line.strip() for line in f if line.strip()]

    batch_size = 50
    batches = [api_ids[i:i + batch_size] for i in range(0, len(api_ids), batch_size)]

    if batch_number > len(batches):
        print(f"No batch number {batch_number} available.")
        return
    
    coins_ids = batches[batch_number - 1]

    all_ohlcv_data = {}
    for coin_id in coins_ids:
        ohlcv_data = get_ohlcv_latest_data(coin_id)
        if ohlcv_data:
            all_ohlcv_data[coin_id] = ohlcv_data[0]
            print(f"Fetched OHLCV data for {coin_id}")
        else:
            print(f"Skipped {coin_id} due to errors.")
    
    # Get current date components
    now = datetime.now(timezone.utc)
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    
    dir_path = os.path.join('/opt/airflow/data/', year, month, day) 
    os.makedirs(dir_path, exist_ok=True)

    # Save all data to a single JSON file
    output_filename = os.path.join(dir_path, f'all_ohlcv_latest_batch_{batch_number}.json')
    with open(output_filename, 'w') as outfile:
        json.dump(all_ohlcv_data, outfile, indent=4)
    print(f"Saved batch {batch_number} OHLCV data to {output_filename}")

process_batch_task = PythonOperator(
    task_id='process_coin_batch',
    python_callable=process_coin_batch,
    provide_context=True,
    dag=dag,
)