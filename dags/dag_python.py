from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def greet():
    print("Hello World")

with DAG(
    dag_id = 'dag_with_python',
    default_args=default_args,
    description='using python operator',
    start_date=datetime(2025, 3, 17, 2),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='first_task',
        python_callable=greet
    )
    
    task1