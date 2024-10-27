from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta, timezone

# Default arguments for the DAG
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
    'process_and_append_parquet',
    default_args=default_args,
    description='DAG to process and append data',
    schedule_interval="0 12 * * *", 
    catchup=False,
)


wait_for_download = ExternalTaskSensor(
    task_id='wait_for_download',
    external_dag_id='fetch_and_upload_ohlcv_data', 
    external_task_id='process_and_upload_task',     
    mode='poke',
    poke_interval=60,  
    timeout=60 * 60,   
    dag=dag,
)

def download_jsons():
    print("JSONs downloaded and ready for processing.")

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

wait_for_download >> download_jsons_task >> append_parquet_task
