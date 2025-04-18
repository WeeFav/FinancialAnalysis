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

WATCHED_DIR = "./datasets/data"
RECORD_FILE = "./datasets/seen_files.txt"

def get_companies(ti):
    tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    
    tickers = tickers['CIK'].to_list()
    # tickers = ['AAPL', 'GOOG', 'MSFT']
    
    ti.xcom_push(key='companies', value=tickers)

def get_new_fs(ti):
    # with open(RECORD_FILE, "r") as f:
    #     seen_files = set(f.read().splitlines())
        
    # s3_hook = S3Hook(aws_conn_id="s3_conn")
    # response = s3_hook.get_conn().list_objects_v2(Bucket="financial-statement-datasets", Delimiter='/')
    
    # if 'CommonPrefixes' in response:
    #     curr_files = [content['Prefix'][:-1] for content in response['CommonPrefixes']]
    # else:
    #     curr_files = []
            
    # new_files = set(curr_files) - seen_files
    
    # new_files = set()
    # for year in range(2020, 2022 + 1):
    #     for qtr in range(1, 4 + 1):
    #         new_files.add(f'{year}q{qtr}')
    new_files = set(['2020q1', '2020q2', '2020q3', '2021q3', '2021q4', '2022q1', '2022q4'])
    
    # with open(RECORD_FILE, "w") as f:
    #     f.write("\n".join(curr_files))
    
    ti.xcom_push(key='new_files', value=list(new_files))
    
def branch(ti):
    new_files = ti.xcom_pull(key='new_files', task_ids=['fs_pipeline.task_get_new_fs'])[0]
    print("new_files", new_files)
    if len(new_files) > 0:
        return 'fs_pipeline.task_extract_fs'
    else:
        return 'fs_pipeline.task_exit'

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

def extract_stock(ti):
    companies = ti.xcom_pull(key='companies', task_ids=['task_get_companies'])[0]
    appl = yf.Tickers(companies)
    stock_price = appl.history(start="2024-01-01", end="2024-12-31", interval="1d")
    stock_price = stock_price.drop(["Open", "High", "Low", "Dividends", "Stock Splits"], axis=1) # drop irrelevant columns
    stock_price = stock_price.stack(level=1).reset_index()
    stock_price['Date'] = stock_price['Date'].dt.date
    stock_price['Volume'] = stock_price['Volume'].astype(np.int32)
    stock_price.to_parquet('./stock.parquet')
    print("Saved parquet file")

def scrape(url):
    header = {'Connection': 'keep-alive',
            'Expires': '-1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
            }
    
    content = ""
    result = requests.get(url, headers=header)
    doc = BeautifulSoup(result.text, "html.parser")
    article = doc.find(class_="article yf-l7apfj")
    tags = article.find_all("p", class_=["yf-1090901", "yf-1ba2ufg"])

    for tag in tags:
        content += tag.text + "\n"

    return content

def extract_news(ti):
    companies = ti.xcom_pull(key='companies', task_ids=['task_get_companies'])[0]
    
    processed_news = []
    
    for company in companies:
        news_list = yf.Search(company, news_count=5).news

        for news in news_list:
            news_dict = {}
            news_dict['ticker'] = company
            news_dict['title'] = news['title']
            news_dict['date'] = datetime.fromtimestamp(news['providerPublishTime']).date().strftime('%Y-%m-%d')
            news_dict['content'] = scrape(news['link'])
            processed_news.append(news_dict)

    with open("./news.json", "w") as fp:
        json.dump(processed_news, fp)    

default_args = {
    'owner': 'admin',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'data_pipeline',
    default_args=default_args,
    description='pipeline for extracting finanical statements, stocks, and news',
    schedule_interval=None
) as dag:
    task_get_companies = PythonOperator(
        task_id="task_get_companies",
        python_callable=get_companies
    )
    
    with TaskGroup('fs_pipeline') as fs_pipeline:    
        task_get_new_fs = PythonOperator(
            task_id="task_get_new_fs",
            python_callable=get_new_fs
        )
        
        task_branch = BranchPythonOperator(
            task_id='task_branch',
            python_callable=branch,
            dag=dag,
        )
        
        task_exit = BashOperator(
            task_id='task_exit',
            bash_command="echo No new files"
        )
        
        task_extract_fs = PythonOperator(
            task_id="task_extract_fs",
            python_callable=extract_fs
        )
        
        # task_fs_to_s3 = PythonOperator(
        #     task_id="task_fs_to_s3",
        #     python_callable=fs_to_s3,
        # )
        
        # task_fs_to_database = PythonOperator(
        #     task_id="task_fs_to_database",
        #     python_callable=fs_to_database,
        # )
        
        task_get_new_fs >> task_branch
        task_branch >> [task_extract_fs, task_exit]
        # task_extract_fs >> task_fs_to_s3 >> task_fs_to_database
        # task_get_new_fs >> task_fs_to_s3 >> task_fs_to_database
    
    # with TaskGroup('stock_pipeline') as stock_pipeline:
    #     task_extract_stock = PythonOperator(
    #         task_id="task_extract_stock",
    #         python_callable=extract_stock
    #     )
        
    #     task_upload_to_s3 = PythonOperator(
    #         task_id="task_upload_to_s3",
    #         python_callable=upload_to_s3,
    #         op_kwargs={
    #             "filename_list": ["./stock.parquet"],
    #             "key_list": ["stock/stock.parquet"]
    #         }
    #     )
        
    #     task_copy_to_redshift = PythonOperator(
    #         task_id="task_copy_to_redshift",
    #         python_callable=copy_to_redshift,
    #         op_kwargs={
    #             "table_list": ["stock"],
    #             "s3_filename_list": ["stock/stock.parquet"]
    #         }
    #     )
        
    #     # task_extract_stock >> task_upload_to_s3 >> task_copy_to_redshift        

    # with TaskGroup('news_pipeline') as news_pipeline:
    #     task_extract_news = PythonOperator(
    #         task_id="task_extract_news",
    #         python_callable=extract_news
    #     )
        
    #     task_upload_to_s3 = PythonOperator(
    #         task_id="task_upload_to_s3",
    #         python_callable=upload_to_s3,
    #         op_kwargs={
    #             "filename_list": ["./news.json"],
    #             "key_list": ["news/news.json"]
    #         }
    #     )
        
    #     # task_extract_news >> task_upload_to_s3        

    # # task_get_companies >> [fs_pipeline, stock_pipeline, news_pipeline]
    task_get_companies >> [fs_pipeline]
    
    
    