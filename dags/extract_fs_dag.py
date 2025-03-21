from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
from sec_cik_mapper import StockMapper
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat_ws, lit, quarter
import redshift_connector

WATCHED_DIR = "./datasets/data"
RECORD_FILE = "./datasets/seen_files.txt"

def get_files(ti):
    with open(RECORD_FILE, "r") as f:
        seen_files = set(f.read().splitlines())
    
    curr_files = set(os.listdir(WATCHED_DIR))
    
    new_files = curr_files - seen_files
    
    with open(RECORD_FILE, "w") as f:
        f.write("\n".join(curr_files))
    
    ti.xcom_push(key='new_files', value=list(new_files))
    
def branch(ti):
    new_files = ti.xcom_pull(key='new_files', task_ids=['task1_get_files'])[0]
    print("new_files", new_files)
    if len(new_files) > 0:
        return 'task2_extract'
    else:
        return 'task_exit'

def extract_new_files(ti):
    new_files = ti.xcom_pull(key='new_files', task_ids=['task1_get_files'])[0]
    
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

def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    s3_hook.load_file(
        filename="./fs.parquet",
        key="fs/fs.parquet",
        bucket_name="financial-analysis-project-bucket",
        replace=True
    )

def copy_to_redshift():
    conn = redshift_connector.connect(
        host=os.getenv('REDSHIFT_HOST'),
        database='dev',
        port=5439,
        user=os.getenv('REDSHIFT_USER'),
        password=os.getenv('REDSHIFT_PASSWORD')
    )

    cursor = conn.cursor()
    cursor.execute("COPY dev.test.fs_test FROM 's3://financial-analysis-project-bucket/fs/fs.parquet' IAM_ROLE 'arn:aws:iam::207567756516:role/service-role/AmazonRedshift-CommandsAccessRole-20250321T104142' FORMAT AS PARQUET")
    conn.commit()     

default_args = {
    'owner': 'admin',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'extract_fs_2',
    default_args=default_args,
    description='pipeline for extracting finanical statements',
    schedule_interval=None
) as dag:
    task1_get_files = PythonOperator(
        task_id="task1_get_files",
        python_callable=get_files
    )
    
    task_branch = BranchPythonOperator(
        task_id='task_branch',
        python_callable=branch,
        dag=dag,
    )
    
    task2_extract = PythonOperator(
        task_id="task2_extract",
        python_callable=extract_new_files
    )
    
    task_exit = BashOperator(
        task_id='task_exit',
        bash_command="echo No new files"
    )
    
    task3_upload_s3 = PythonOperator(
        task_id="task3_upload",
        python_callable=upload_to_s3
    )
    
    task4_copy_redshift = PythonOperator(
        task_id="task4_copy_redshift",
        python_callable=copy_to_redshift
    )
    
    task1_get_files >> task_branch
    task_branch >> [task2_extract, task_exit]
    task2_extract >> task3_upload_s3 >> task4_copy_redshift
    