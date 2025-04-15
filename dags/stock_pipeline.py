from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import os
from sec_cik_mapper import StockMapper
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat_ws, lit, quarter, to_date, year
import redshift_connector
import yfinance as yf
import numpy as np
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import io
from pyspark import SparkConf
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import time

def get_companies(ti):
    tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    tickers = tickers['Symbol'].to_list()
    ti.xcom_push(key='companies', value=tickers)

def extract_stock(ti):
    companies = ti.xcom_pull(key='companies', task_ids=['task_get_companies'])[0]
    appl = yf.Tickers(companies)
    stock_price = appl.history(start="2009-01-01", end="2024-12-31", interval="1d")
    stock_price = stock_price.drop(["Dividends", "Stock Splits"], axis=1) # drop irrelevant columns
    stock_price = stock_price.stack(level=1).reset_index()
    stock_price["Date"] = stock_price["Date"].dt.date
    stock_price['Volume'] = stock_price['Volume'].astype(np.int32)
    stock_price.to_parquet('./stock.parquet')
    print("Saved parquet file")

def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    
    s3_hook.load_file(
        filename='./stock.parquet',
        key="fs/stock/stock.parquet",
        bucket_name="financial-analysis-project-bucket",
        replace=True
    )

def copy_to_redshift(folder):
    conn = redshift_connector.connect(
        host=os.getenv('REDSHIFT_HOST'),
        database='dev',
        port=5439,
        user=os.getenv('REDSHIFT_USER'),
        password=os.getenv('REDSHIFT_PASSWORD')
    )
    
    cursor = conn.cursor()
    cursor.execute(f"COPY dev.test.fs_sub FROM 's3://financial-analysis-project-bucket/fs/{folder}/sub.parquet' IAM_ROLE 'arn:aws:iam::207567756516:role/service-role/AmazonRedshift-CommandsAccessRole-20250321T104142' FORMAT AS PARQUET")
    cursor.execute(f"COPY dev.test.fs_num FROM 's3://financial-analysis-project-bucket/fs/{folder}/num.parquet' IAM_ROLE 'arn:aws:iam::207567756516:role/service-role/AmazonRedshift-CommandsAccessRole-20250321T104142' FORMAT AS PARQUET")
    conn.commit() 
    
def copy_to_postgres():
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/financial_statements')
    
    response = s3_hook.get_conn().get_object(Bucket="financial-analysis-project-bucket", Key=f"fs/stock/stock.parquet")
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
    schedule_interval=None
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
    
    task_copy_to_postgres = PythonOperator(
        task_id="task_copy_to_postgres",
        python_callable=copy_to_postgres,
    )
    
    task_extract_stock >> task_upload_to_s3 >> task_copy_to_postgres      

    
    
    
    