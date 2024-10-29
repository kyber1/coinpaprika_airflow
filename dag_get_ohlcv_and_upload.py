from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta, timezone

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

def execution_date_fn_run1(context):
    execution_date = context['execution_date'].replace(hour=10, minute=55, second=0, microsecond=0)
    return execution_date

def execution_date_fn_run2(context):
    execution_date = context['execution_date'].replace(hour=11, minute=55, second=0, microsecond=0)
    return execution_date

wait_for_fetch_data_run1 = ExternalTaskSensor(
    task_id='wait_for_fetch_data_run1',
    external_dag_id='fetch_and_upload_ohlcv_data',
    external_task_id='process_and_upload_task',
    execution_date_fn=execution_date_fn_run1,
    mode='reschedule',
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    dag=dag,
)

wait_for_fetch_data_run2 = ExternalTaskSensor(
    task_id='wait_for_fetch_data_run2',
    external_dag_id='fetch_and_upload_ohlcv_data',
    external_task_id='process_and_upload_task',
    execution_date_fn=execution_date_fn_run2,
    mode='reschedule',
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    dag=dag,
)

def download_jsons(**kwargs):
    print("JSONs downloaded and ready for processing.")

def append_to_parquet(**kwargs):
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

[wait_for_fetch_data_run1, wait_for_fetch_data_run2] >> download_jsons_task >> append_parquet_task
