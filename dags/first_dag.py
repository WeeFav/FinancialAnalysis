from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'first_dag_v3',
    default_args=default_args,
    description='this is first dag',
    start_date=datetime(2025, 3, 17, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world"
    )
    
    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo task2"
    )
    
    task3 = BashOperator(
        task_id='thrid_task',
        bash_command="echo task3"
    )
    
    task1 >> [task2, task3]