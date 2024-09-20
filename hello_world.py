from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def HelloWorld():
    print("Hello Wolrd")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,9,19),
}

with DAG('hello_world_dag', default_args=default_args, schedule_interval="@hourly", catchup=False) as dag:
    hello_task = PythonOperator(
        task_id = 'hello_world_task',
        python_callable = HelloWorld
    )
