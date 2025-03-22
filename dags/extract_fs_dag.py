from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
from sec_cik_mapper import StockMapper
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat_ws, lit, quarter
import redshift_connector
import yfinance as yf
import numpy as np
from bs4 import BeautifulSoup
import requests
import json

WATCHED_DIR = "./datasets/data"
RECORD_FILE = "./datasets/seen_files.txt"

def get_new_fs(ti):
    with open(RECORD_FILE, "r") as f:
        seen_files = set(f.read().splitlines())
    
    curr_files = set(os.listdir(WATCHED_DIR))
    
    new_files = curr_files - seen_files
    
    with open(RECORD_FILE, "w") as f:
        f.write("\n".join(curr_files))
    
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
    
    sub_files = [os.path.join("./datasets/data", folder, "sub.txt") for folder in new_files]
    num_files = [os.path.join("./datasets/data", folder, "num.txt") for folder in new_files]
    
    mapper = StockMapper()
    ticker_to_cik = mapper.ticker_to_cik

    spark: SparkSession = SparkSession.builder.getOrCreate() # create spark session
    sub_df = spark.read.csv(sub_files, sep='\t', header=True, inferSchema=True)
    sub_df = sub_df.filter((sub_df.cik == ticker_to_cik['AAPL']) & ((sub_df.form == "10-Q") | (sub_df.form == "10-K")))
    sub_df.show()

    num_df = spark.read.csv(num_files, sep='\t', header=True, inferSchema=True) # read financial data
    num_df = num_df.join(sub_df, "adsh", "left_semi")
    num_df = num_df.filter( 
                (num_df.tag.isin(["RevenueFromContractWithCustomerExcludingAssessedTax", "CostOfGoodsAndServicesSold", "GrossProfit", "ResearchAndDevelopmentExpense", "SellingGeneralAndAdministrativeExpense", "OperatingExpenses", "OperatingIncomeLoss", "NetIncomeLoss"]) ) &
                # (num_df.segments.isNull()) &
                # (num_df.qtrs == 1) &
                (num_df.ddate.startswith("2024")))
    # # drop irrevelant columns
    num_df = num_df.drop("version", "ddate", "uom", "coreg", "footnote")
    # # extract revenue year
    num_df = num_df.withColumn("value", col("value") / 1000000)
                                
    # num_df = num_df.select("adsh", "tag", "value")
    num_df.show()

    num_df.toPandas().to_parquet("./fs.parquet")
    print("Saved parquet file")

def upload_to_s3(filename, key):
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    s3_hook.load_file(
        filename=filename,
        key=key,
        bucket_name="financial-analysis-project-bucket",
        replace=True
    )

def copy_to_redshift(table, s3_filename):
    conn = redshift_connector.connect(
        host=os.getenv('REDSHIFT_HOST'),
        database='dev',
        port=5439,
        user=os.getenv('REDSHIFT_USER'),
        password=os.getenv('REDSHIFT_PASSWORD')
    )

    cursor = conn.cursor()
    cursor.execute(f"COPY dev.test.{table} FROM 's3://financial-analysis-project-bucket/{s3_filename}' IAM_ROLE 'arn:aws:iam::207567756516:role/service-role/AmazonRedshift-CommandsAccessRole-20250321T104142' FORMAT AS PARQUET")
    conn.commit() 

def extract_stock():
    appl = yf.Ticker("AAPL")
    stock_price = appl.history(start="2024-01-01", end="2024-12-31", interval="1d")
    stock_price = stock_price.drop(["Open", "High", "Low", "Dividends", "Stock Splits"], axis=1) # drop irrelevant columns
    stock_price.index = stock_price.index.date # convert datetime index into date
    stock_price = stock_price.reset_index() # reset index to default integer index and move existing date index into a column
    stock_price = stock_price.rename(columns={'index':'Date'})
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

def extract_news():
    news_list = yf.Search("AAPL", news_count=5).news

    processed_news = []

    for news in news_list:
        news_dict = {}
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
        
        task_upload_to_s3 = PythonOperator(
            task_id="task_upload_to_s3",
            python_callable=upload_to_s3,
            op_kwargs={
                "filename": "./fs.parquet",
                "key": "fs/fs.parquet"
            }
        )
        
        task_copy_to_redshift = PythonOperator(
            task_id="task_copy_to_redshift",
            python_callable=copy_to_redshift,
            op_kwargs={
                "table": "fs_test",
                "s3_filename": "fs/fs.parquet"
            }
        )
        
        task_get_new_fs >> task_branch
        task_branch >> [task_extract_fs, task_exit]
        task_extract_fs >> task_upload_to_s3 >> task_copy_to_redshift
    
    with TaskGroup('stock_pipeline') as stock_pipeline:
        task_extract_stock = PythonOperator(
            task_id="task_extract_stock",
            python_callable=extract_stock
        )
        
        task_upload_to_s3 = PythonOperator(
            task_id="task_upload_to_s3",
            python_callable=upload_to_s3,
            op_kwargs={
                "filename": "./stock.parquet",
                "key": "stock/stock.parquet"
            }
        )
        
        task_copy_to_redshift = PythonOperator(
            task_id="task_copy_to_redshift",
            python_callable=copy_to_redshift,
            op_kwargs={
                "table": "stock_test",
                "s3_filename": "stock/stock.parquet"
            }
        )
        
        task_extract_stock >> task_upload_to_s3 >> task_copy_to_redshift        

    with TaskGroup('news_pipeline') as news_pipeline:
        task_extract_news = PythonOperator(
            task_id="task_extract_news",
            python_callable=extract_news
        )
        
        task_upload_to_s3 = PythonOperator(
            task_id="task_upload_to_s3",
            python_callable=upload_to_s3,
            op_kwargs={
                "filename": "./news.json",
                "key": "news/news.json"
            }
        )
        
        task_extract_news >> task_upload_to_s3        

    fs_pipeline
    stock_pipeline
    news_pipeline