from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
import redshift_connector
import yfinance as yf
import numpy as np
import requests
import pandas as pd
import io
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from curl_cffi import requests
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param

# postgres database connection
postgres_conn_id = 'postgres_conn'
postgreshook = PostgresHook(postgres_conn_id)
postgres_conn = postgreshook.get_conn()
# redshift database connection
redshift_conn_id = 'redshift_conn'
redshifthook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
# since get_conn() somehow doesn't work on RedshiftSQLHook, we need to work around
redshift_conn = redshift_connector.connect(
    host=redshifthook.conn.host,
    database=redshifthook.conn.schema,
    port=redshifthook.conn.port,
    user=redshifthook.conn.login,
    password=redshifthook.conn.password
)

def branch(connection_type):
    """Branch to redshift or postgres"""
    
    if connection_type == "redshift":
        return 'task_copy_to_redshift'
    elif connection_type == "postgres":
        return 'task_copy_to_postgres'

def get_companies(ti):
    """Get S&P 500 companies symbols from wikipedia and S&P Index symbol"""
    tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    tickers = tickers['Symbol'].to_list() + ['^GSPC']
    ti.xcom_push(key='companies', value=tickers)

def extract_stock(ti):
    """Extract stock prices from yahoo finance, transform the data, and save it locally"""
    
    companies = ti.xcom_pull(key='companies', task_ids=['task_get_companies'])[0]
    session = requests.Session(impersonate="chrome")
    tickers = yf.Tickers(companies, session=session)
    
    stock_price = tickers.history(start="2009-01-01", end="2024-12-31", interval="1d")
    stock_price = stock_price.drop(["Dividends", "Stock Splits"], axis=1)
    stock_price = stock_price.stack(level=1).reset_index()
    stock_price["Date"] = stock_price["Date"].dt.date
    stock_price['Volume'] = stock_price['Volume'].astype(np.int32)
    
    stock_price.to_parquet('./stock.parquet')
    print("Saved parquet file")

def upload_to_s3():
    """Upload parquet to S3"""
    
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    s3_hook.load_file(
        filename='./stock.parquet',
        key="stock/stock.parquet",
        bucket_name="financial-analysis-project-bucket",
        replace=True
    )

def copy_to_redshift():
    """Load parquet to redshift database"""
    
    cursor = redshift_conn.cursor()
    cursor.execute(f"COPY dev.public.stock FROM 's3://financial-analysis-project-bucket/stock/stock.parquet' IAM_ROLE 'arn:aws:iam::207567756516:role/service-role/AmazonRedshift-CommandsAccessRole-20250321T104142' FORMAT AS PARQUET")
    redshift_conn.commit() 
    
    print("Upload Success!")
    
def copy_to_postgres():
    """Load parquet to local postgres database"""
    
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/financial_statements')
    
    response = s3_hook.get_conn().get_object(Bucket="financial-analysis-project-bucket", Key=f"stock/stock.parquet")
    data = response['Body'].read()
    table = pq.read_table(io.BytesIO(data))
    df = table.to_pandas()
    df.to_sql("stock", engine, if_exists='replace', index=False)

    print("Upload Success!")

default_args = {
    'owner': 'admin',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'stock_pipeline',
    default_args=default_args,
    description='pipeline for extracting stocks',
    schedule_interval=None,
    params={
        'connection_type': Param("postgres", enum=["postgres", "redshift"])
    }
) as dag:
    
    task_get_companies = PythonOperator(
        task_id="task_get_companies",
        python_callable=get_companies
    )
    
    task_extract_stock = PythonOperator(
        task_id="task_extract_stock",
        python_callable=extract_stock
    )
    
    task_upload_to_s3 = PythonOperator(
        task_id="task_upload_to_s3",
        python_callable=upload_to_s3,
    )
    
    task_branch = BranchPythonOperator(
        task_id='task_branch',
        python_callable=branch,
        op_kwargs={
            "connection_type": "{{ params.connection_type }}",
        },
        dag=dag
    )
    
    task_copy_to_postgres = PythonOperator(
        task_id="task_copy_to_postgres",
        python_callable=copy_to_postgres,
    )
    
    task_copy_to_redshift = PythonOperator(
        task_id="task_copy_to_redshift",
        python_callable=copy_to_redshift,
    )
    
    task_get_companies >> task_extract_stock >> task_upload_to_s3 >> task_branch
    task_branch >> [task_copy_to_redshift, task_copy_to_postgres]      
    
    