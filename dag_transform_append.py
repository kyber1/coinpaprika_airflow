from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import gcsfs
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 2, tzinfo=timezone.utc),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_and_append_parq',
    default_args=default_args,
    description='DAG to process and append data',
    schedule_interval="59 11 * * *", 
    catchup=False,
)

wait_for_first_run = ExternalTaskSensor(
    task_id='wait_for_first_run',
    external_dag_id='fetch_and_upload_ohlcv_data',
    external_task_id='process_and_upload_task',
    execution_date_fn=lambda x: x.replace(hour=10, minute=55),
    mode='poke',
    timeout=7200, 
    dag=dag,
)

wait_for_second_run = ExternalTaskSensor(
    task_id='wait_for_second_run',
    external_dag_id='fetch_and_upload_ohlcv_data',
    external_task_id='process_and_upload_task',
    execution_date_fn=lambda x: x.replace(hour=11, minute=55),
    mode='poke',
    timeout=7200, 
    dag=dag,
)

def download_jsons():
    project_id = 'tough-bearing-436219-r4'
    bronze_bucket_name = 'coinpaprika_bronze'

    fs = gcsfs.GCSFileSystem(project=project_id)

    today = datetime.now(timezone.utc)
    today_path = today.strftime(f'{bronze_bucket_name}/%Y/%m/%d')

    files = fs.glob(f'gs://{today_path}/*.json')
    print(f"Files found: {files}")

    combined_data = []

    for file_path in files:
        print(f"Processing file: {file_path}")
        with fs.open(file_path, 'r') as f:
            data = json.load(f)
            print(f"Data loaded from {file_path}: {data}")

            for crypto_name, coin_values in data.items():
                entry = {
                    'timestamp': coin_values.get('time_open'),
                    'cryptocurrency': crypto_name,
                    'open': coin_values.get('open'),
                    'close': coin_values.get('close'),
                    'high': coin_values.get('high'),
                    'low': coin_values.get('low'),
                    'volume': coin_values.get('volume'),
                    'market_cap': coin_values.get('market_cap')
                }
                combined_data.append(entry)
                print(f"Added entry: {entry}")

    df = pd.DataFrame(combined_data)
    print(f"Combined DataFrame:\n{df.head()}")
    print(f"DataFrame shape: {df.shape}")

    return df

def append_to_parquet():
    print("Data processed and appended to Parquet.")

download_jsons_task = PythonOperator(
    task_id='download_jsons',
    python_callable=download_jsons,
    dag=dag,
)

append_parquet_task = PythonOperator(
    task_id='append_to_parquet',
    python_callable=append_to_parquet,
    dag=dag,
)

(wait_for_first_run >> wait_for_second_run) >> download_jsons_task >> append_parquet_task
