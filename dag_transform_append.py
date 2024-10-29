from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagRun
from airflow.utils.state import State
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

def check_two_runs_success(**kwargs):
    dag_id = 'fetch_and_upload_ohlcv_data'
    today = datetime.now(timezone.utc).date()
    
    all_runs = DagRun.find(dag_id=dag_id)

    successful_runs_today = [
        run for run in all_runs 
        if run.state == State.SUCCESS and run.execution_date.date() == today
    ]

    if len(successful_runs_today) >= 2:
        return True
    else:
        raise ValueError("Both required DAG runs are not yet successful.")

check_runs = PythonOperator(
    task_id='check_two_runs_success',
    python_callable=check_two_runs_success,
    provide_context=True,
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

check_runs >> download_jsons_task >> append_parquet_task
