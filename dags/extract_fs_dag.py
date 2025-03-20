from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
from sec_cik_mapper import StockMapper
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat_ws, lit, quarter

WATCHED_DIR = "./datasets"
RECORD_FILE = "./seen_files.txt"

def get_files():
    with open(RECORD_FILE, "r") as f:
        seen_files = set(f.read().splitlines())
    
    curr_files = set(os.listdir(WATCHED_DIR))
    
    new_files = curr_files - seen_files
    
    with open(RECORD_FILE, "w") as f:
        f.write("\n".join(curr_files))
    
    return new_files

def extract():
    mapper = StockMapper()
    ticker_to_cik = mapper.ticker_to_cik

    spark: SparkSession = SparkSession.builder.getOrCreate() # create spark session
    sub_df = spark.read.csv("./datasets/2024q2/sub.txt", sep='\t', header=True, inferSchema=True)
    sub_df = sub_df.filter((sub_df.cik == ticker_to_cik['AAPL']) & (sub_df.form == "10-Q"))

    num_df = spark.read.csv("./datasets/2024q2/num.txt", sep='\t', header=True, inferSchema=True) # read financial data
    num_df = num_df.filter((num_df.adsh == sub_df.first().adsh) & 
                (num_df.tag.isin(["RevenueFromContractWithCustomerExcludingAssessedTax", "CostOfGoodsAndServicesSold", "GrossProfit", "ResearchAndDevelopmentExpense", "SellingGeneralAndAdministrativeExpense", "OperatingExpenses", "OperatingIncomeLoss", "NetIncomeLoss"]) ) &
                # (num_df.segments.isNull()) &
                (num_df.qtrs == 1) &
                (num_df.ddate.startswith("2024")))
    # # drop irrevelant columns
    num_df = num_df.drop("adsh", "version", "qtrs", "uom", "segments", "coreg", "footnote")
    # # extract revenue year
    num_df = num_df.withColumn("year", substring(col("ddate"), 1, 4)) \
                .withColumn("value", col("value") / 1000000)
                                
    num_df = num_df.select("tag", "year", "value")
    num_df.show()

    num_df.toPandas().to_parquet("./fs.parquet")
    
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    s3_hook.load_file(
        filename="./fs.parquet",
        key="fs/fs.parquet",
        bucket_name="financial-analysis-project-bucket",
        replace=True
    )   

default_args = {
    'owner': 'admin',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'extract_fs',
    default_args=default_args,
    description='pipeline for extracting finanical statements',
    schedule_interval=None
) as dag:
    task1 = PythonOperator(
        task_id="extract",
        python_callable=extract
    )
    task1