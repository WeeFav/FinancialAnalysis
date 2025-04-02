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


def extract_fs():
    mapper = StockMapper()
    ticker_to_cik = mapper.ticker_to_cik
    tags = ["RevenueFromContractWithCustomerExcludingAssessedTax", "CostOfGoodsAndServicesSold", "GrossProfit", "ResearchAndDevelopmentExpense", "SellingGeneralAndAdministrativeExpense", "OperatingExpenses", "OperatingIncomeLoss", "NetIncomeLoss"]
    
    companies_cik = [int(ticker_to_cik[company]) for company in ['AAPL', 'GOOG', 'MSFT']]
    
    aws_conn = BaseHook.get_connection("aws_conn")
    conf = SparkConf()
    conf.set("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.2.0")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.hadoop.fs.s3a.access.key", aws_conn.login)
    conf.set("spark.hadoop.fs.s3a.secret.key", aws_conn.password)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    while True:
        sub_df = spark.read.csv(f"s3a://financial-statement-datasets/2024q4/sub.txt", sep='\t', header=True, inferSchema=True)
        sub_df.cache()
        sub_df = sub_df.filter((sub_df.cik.isin(companies_cik)) & (sub_df.form.isin(["10-Q", "10-K"])))
        sub_df = sub_df.drop("cik", "sic", "zipba", "bas1", "bas2", "baph", "countryma", "stprma", "cityma", "zipma", "mas1", "mas2", "countryinc", "stprinc", "ein", "former", "changed", "afs", "wksi", "filed", "accepted", "prevrpt", "detail", "instance", "nciks", "aciks")
        sub_df = sub_df.withColumn("period", to_date("period", 'yyyyMMdd'))
        sub_df.show()

        num_df = spark.read.csv(f"s3a://financial-statement-datasets/2024q4/num.txt", sep='\t', header=True, inferSchema=True)        
        num_df.cache()
        num_df = num_df.withColumn("ddate", substring("ddate", 0, 4)).join(sub_df.withColumn("ddate", year("period")), ["adsh", "ddate"], "left_semi")
        num_df = num_df.filter((num_df.tag.isin(tags)))
        num_df = num_df.drop("version", "ddate", "uom", "coreg", "footnote")
        num_df = num_df.withColumn("value", col("value") / 1000000) # convert values with unit of millions 
        num_df.show()
        
        num_df.toPandas().to_parquet(f"./num.parquet")
        sub_df.toPandas().to_parquet(f"./sub.parquet")
        print(f"Saved parquet file")
        
def extract_fs_local():
    mapper = StockMapper()
    ticker_to_cik = mapper.ticker_to_cik
    tags = ["RevenueFromContractWithCustomerExcludingAssessedTax", "CostOfGoodsAndServicesSold", "GrossProfit", "ResearchAndDevelopmentExpense", "SellingGeneralAndAdministrativeExpense", "OperatingExpenses", "OperatingIncomeLoss", "NetIncomeLoss"]
    
    companies_cik = [int(ticker_to_cik[company]) for company in ['AAPL', 'GOOG', 'MSFT']]
    
    aws_conn = BaseHook.get_connection("aws_conn")
    conf = SparkConf()
    conf.set("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.2.0")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.hadoop.fs.s3a.access.key", aws_conn.login)
    conf.set("spark.hadoop.fs.s3a.secret.key", aws_conn.password)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    sub_df = spark.read.csv("./datasets/sub.txt", sep='\t', header=True, inferSchema=True)
    sub_df = sub_df.filter((sub_df.cik.isin(companies_cik)) & (sub_df.form.isin(["10-Q", "10-K"])))
    sub_df = sub_df.drop("zipba", "bas1", "bas2", "baph", "countryma", "stprma", "cityma", "zipma", "mas1", "mas2", "ein", "former", "changed", "afs", "wksi", "filed", "accepted", "prevrpt", "detail", "instance", "nciks", "aciks")
    sub_df = sub_df.withColumn("period", to_date("period", 'yyyyMMdd'))
    sub_df.show()   

    num_df = spark.read.csv(f"./datasets/num.txt", sep='\t', header=True, inferSchema=True)
    num_df = num_df.withColumn("ddate", to_date("ddate", 'yyyyMMdd'))        
    num_df = num_df.join(sub_df, (num_df["adsh"] == sub_df["adsh"]) & (year(num_df["ddate"]) == year(sub_df["period"])), "left_semi")
    # num_df = num_df.filter((num_df.tag.isin(tags)))
    num_df = num_df.drop("version", "coreg", "footnote") 
    num_df.show()
    
    num_df.toPandas().to_parquet(f"./num.parquet")
    sub_df.toPandas().to_parquet(f"./sub.parquet")
    print(f"Saved parquet file")
        
default_args = {
    'owner': 'admin',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'test_dag',
    default_args=default_args,
    description='test',
    schedule_interval=None
) as dag:
    
    # task1 = PythonOperator(
    #     task_id="task1",
    #     python_callable=extract_fs
    # )
    
    task1 = PythonOperator(
        task_id="task1",
        python_callable=extract_fs_local
    )

    task1
    
    
    