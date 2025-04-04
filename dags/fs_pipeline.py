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
    
    tickers = tickers['CIK'].to_list()
    # tickers = ['AAPL', 'GOOG', 'MSFT']
    
    ti.xcom_push(key='companies', value=tickers)

def extract_fs(ti):
    new_files = ti.xcom_pull(key='new_files', task_ids=['fs_pipeline.task_get_new_fs'])[0]
    companies = ti.xcom_pull(key='companies', task_ids=['task_get_companies'])[0]
    
    mapper = StockMapper()
    ticker_to_cik = mapper.ticker_to_cik
    # tags = ["RevenueFromContractWithCustomerExcludingAssessedTax", "CostOfGoodsAndServicesSold", "GrossProfit", "ResearchAndDevelopmentExpense", "SellingGeneralAndAdministrativeExpense", "OperatingExpenses", "OperatingIncomeLoss", "NetIncomeLoss"]
    
    # companies_cik = [int(ticker_to_cik[company]) for company in companies]
    print(len(companies))
    companies_cik = companies
    
    aws_conn = BaseHook.get_connection("aws_conn")
    conf = SparkConf()
    conf.set("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.2.0")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.hadoop.fs.s3a.access.key", aws_conn.login)
    conf.set("spark.hadoop.fs.s3a.secret.key", aws_conn.password)
    conf.set("spark.sql.shuffle.partitions", "200")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
        
    for file in new_files:
        start_time = time.time()
        sub_df = spark.read.csv(f"s3a://financial-statement-datasets/{file}/sub.txt", sep='\t', header=True, inferSchema=True)
        read_time = time.time()
        print(start_time - read_time)
        sub_df.cache()
        sub_df = sub_df.filter((sub_df.cik.isin(companies_cik)) & (sub_df.form.isin(["10-Q", "10-K"])))
        sub_df = sub_df.drop("zipba", "bas1", "bas2", "baph", "countryma", "stprma", "cityma", "zipma", "mas1", "mas2", "ein", "former", "changed", "afs", "wksi", "filed", "accepted", "prevrpt", "detail", "instance", "nciks", "aciks")
        sub_df = sub_df.withColumn("period", to_date("period", 'yyyyMMdd'))
        process_time = time.time()
        print(read_time - process_time)
        sub_df.show()   
        
        start_time = time.time()
        num_df = spark.read.csv(f"s3a://financial-statement-datasets/{file}/num.txt", sep='\t', header=True, inferSchema=True)        
        read_time = time.time()
        print(start_time - read_time)
        num_df.cache()
        num_df = num_df.withColumn("ddate", to_date("ddate", 'yyyyMMdd'))        
        num_df = num_df.join(sub_df, (num_df["adsh"] == sub_df["adsh"]) & (year(num_df["ddate"]) == year(sub_df["period"])), "left_semi")
        # num_df = num_df.filter((num_df.tag.isin(tags)))
        num_df = num_df.drop("version", "coreg", "footnote") 
        process_time = time.time()
        print(read_time - process_time)
        num_df.show()
        
        os.makedirs(f"./{file}", exist_ok=True)

        num_df.toPandas().to_parquet(f"./{file}/num.parquet")
        sub_df.toPandas().to_parquet(f"./{file}/sub.parquet")
        print(f"Saved {file} parquet file")

def upload_to_s3(filename_list, key_list):
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    
    for i in range(len(filename_list)):
        s3_hook.load_file(
            filename=filename_list[i],
            key=key_list[i],
            bucket_name="financial-analysis-project-bucket",
            replace=True
        )

def fs_to_s3(ti):
    new_files = ti.xcom_pull(key='new_files', task_ids=['fs_pipeline.task_get_new_fs'])[0]
    
    # upload num.parquet
    num_filename_list = [f"./{file}/num.parquet" for file in new_files]
    num_key_list = [f"fs/{file}/num.parquet" for file in new_files]
    
    # upload sub.parquet   
    sub_filename_list = [f"./{file}/sub.parquet" for file in new_files]
    sub_key_list = [f"fs/{file}/sub.parquet" for file in new_files]
    
    upload_to_s3(num_filename_list + sub_filename_list, num_key_list + sub_key_list)
    ti.xcom_push(key='num_key_list', value=num_key_list)
    ti.xcom_push(key='sub_key_list', value=sub_key_list)

def copy_to_redshift(table_list, s3_filename_list):
    conn = redshift_connector.connect(
        host=os.getenv('REDSHIFT_HOST'),
        database='dev',
        port=5439,
        user=os.getenv('REDSHIFT_USER'),
        password=os.getenv('REDSHIFT_PASSWORD')
    )
    
    cursor = conn.cursor()
    
    for i in range(len(table_list)):
        cursor.execute(f"COPY dev.test.{table_list[i]} FROM 's3://financial-analysis-project-bucket/{s3_filename_list[i]}' IAM_ROLE 'arn:aws:iam::207567756516:role/service-role/AmazonRedshift-CommandsAccessRole-20250321T104142' FORMAT AS PARQUET")
    
    conn.commit() 
    
def copy_to_postgres(table_list, s3_filename_list):
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/financial_statements')
    
    for i in range(len(table_list)):
        response = s3_hook.get_conn().get_object(Bucket="financial-analysis-project-bucket", Key=s3_filename_list[i])
        data = response['Body'].read()
        table = pq.read_table(io.BytesIO(data))
        df = table.to_pandas()
        df.to_sql(f"{table_list[i]}", engine, if_exists='append', index=False)

def fs_to_database(ti):
    num_key_list = ti.xcom_pull(key='num_key_list', task_ids=['fs_pipeline.task_fs_to_s3'])[0]
    sub_key_list = ti.xcom_pull(key='sub_key_list', task_ids=['fs_pipeline.task_fs_to_s3'])[0]
    table_list = ['fs_num'] * len(num_key_list) + ['fs_sub'] * len(sub_key_list)
    s3_filename_list = num_key_list + sub_key_list
    
    # copy_to_redshift(table_list, s3_filename_list)
    copy_to_postgres(table_list, s3_filename_list)

default_args = {
    'owner': 'admin',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'data_pipeline',
    default_args=default_args,
    description='pipeline for extracting finanical statements',
    schedule_interval=None,
    params={
        'folder': '2024q1'
    }
) as dag:
    
    task_get_companies = PythonOperator(
        task_id="task_get_companies",
        python_callable=get_companies
    )
    
    task_extract_fs = PythonOperator(
        task_id="task_extract_fs",
        python_callable=extract_fs
    )
    
    task_fs_to_s3 = PythonOperator(
        task_id="task_fs_to_s3",
        python_callable=fs_to_s3,
    )
    
    task_fs_to_database = PythonOperator(
        task_id="task_fs_to_database",
        python_callable=fs_to_database,
    )