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

def read_combine_and_append_to_master():
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

    combined_df = pd.DataFrame(combined_data)
    print(f"Combined DataFrame:\n{combined_df.head()}")

    print("------Reading done, moving to appending------")

    silver_bucket_name = 'coinpaprika_silver'
    master_parquet_path = f'gs://{silver_bucket_name}/ohlcv_data_master.parquet'

    
    if fs.exists(master_parquet_path):
        print(f"loading master parq file: {master_parquet_path}")
        existing_df = pd.read_parquet(master_parquet_path, filesystem=fs)
        
        combined_df = pd.concat([existing_df, combined_df], ignore_index=True)
        print("data appended to df")
    else:
        print("no master file found")

    with fs.open(master_parquet_path, 'wb') as f:
        combined_df.to_parquet(f, index=False)
        print(f"appending, writing to master parq file: {master_parquet_path}")

    print("------Updated master parq fie content------")
    print(combined_df.head())


read_combine_and_append_to_master_task = PythonOperator(
    task_id='read_combine_and_append_to_master',
    python_callable=read_combine_and_append_to_master
    dag=dag,
)

# (wait_for_first_run >> wait_for_second_run) >> read_combine_and_append_to_master
read_combine_and_append_to_master_task 
