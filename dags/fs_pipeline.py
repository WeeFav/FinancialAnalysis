from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat_ws, lit, quarter, to_date, year
import redshift_connector
import pandas as pd
import io
from pyspark import SparkConf
from sqlalchemy import create_engine, text
import pyarrow.parquet as pq
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

def branch1(folder, connection_type):
    if connection_type == "redshift":
        cursor = redshift_conn.cursor()
    elif connection_type == "postgres":
        cursor = postgres_conn.cursor()
  
    cursor.execute("""
                    SELECT * FROM recorded
                    WHERE folder_name = %s
                    """, folder)
            
    rows = cursor.fetchall()
        
    if len(rows) > 0:
        return 'task_exit'
    else:
        return 'task_get_companies'

def branch2(connection_type):
    if connection_type == "redshift":
        return 'task_copy_to_redshift'
    elif connection_type == "postgres":
        return 'task_copy_to_postgres'
        
def get_companies(ti):
    tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    
    tickers = tickers['CIK'].to_list()
    # tickers = ['AAPL', 'GOOG', 'MSFT']
    
    ti.xcom_push(key='companies', value=tickers)

def extract_fs(ti, folder):
    companies = ti.xcom_pull(key='companies', task_ids=['task_get_companies'])[0]
    
    print(len(companies))
    companies_cik = companies
    
    aws_conn = BaseHook.get_connection("aws_default")
    conf = SparkConf()
    conf.set("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.2.0")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.hadoop.fs.s3a.access.key", aws_conn.login)
    conf.set("spark.hadoop.fs.s3a.secret.key", aws_conn.password)
    conf.set("spark.sql.shuffle.partitions", "200")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    sub_df = spark.read.csv(f"s3a://financial-statement-datasets/{folder}/sub.txt", sep='\t', header=True, inferSchema=True)
    sub_df.cache()
    sub_df = sub_df.filter((sub_df.cik.isin(companies_cik)) & (sub_df.form.isin(["10-Q", "10-K"])))
    sub_df = sub_df.drop("zipba", "bas1", "bas2", "baph", "countryma", "stprma", "cityma", "zipma", "mas1", "mas2", "ein", "former", "changed", "afs", "wksi", "filed", "accepted", "prevrpt", "detail", "instance", "nciks", "aciks")
    sub_df = sub_df.withColumn("period", to_date("period", 'yyyyMMdd')).withColumn("folder", lit(folder))
    sub_df = sub_df.select([sub_df.columns[-1]] + sub_df.columns[:-1])
    sub_df.show()   
            
    num_df = spark.read.csv(f"s3a://financial-statement-datasets/{folder}/num.txt", sep='\t', header=True, inferSchema=True)        
    num_df.cache()
    num_df = num_df.withColumn("ddate", to_date("ddate", 'yyyyMMdd'))        
    num_df = num_df.join(sub_df, (num_df["adsh"] == sub_df["adsh"]) & (year(num_df["ddate"]) == year(sub_df["period"])), "left_semi")
    num_df = num_df.drop("version", "coreg", "footnote") 
    num_df.show()
    
    os.makedirs(f"./{folder}", exist_ok=True)

    sub_df.toPandas().to_parquet(f"./{folder}/sub.parquet")
    num_df.toPandas().to_parquet(f"./{folder}/num.parquet")
    print(f"Saved {folder}")

def upload_to_s3(folder):
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    
    s3_hook.load_file(
        filename=f"./{folder}/sub.parquet",
        key=f"fs/{folder}/sub.parquet",
        bucket_name="financial-analysis-project-bucket",
        replace=True
    )
      
    s3_hook.load_file(
        filename=f"./{folder}/num.parquet",
        key=f"fs/{folder}/num.parquet",
        bucket_name="financial-analysis-project-bucket",
        replace=True
    )  
    
def copy_to_redshift(folder):    
    cursor = redshift_conn.cursor()
    cursor.execute(f"COPY dev.public.fs_sub FROM 's3://financial-analysis-project-bucket/fs/{folder}/sub.parquet' IAM_ROLE 'arn:aws:iam::207567756516:role/service-role/AmazonRedshift-CommandsAccessRole-20250321T104142' FORMAT AS PARQUET")
    cursor.execute(f"COPY dev.public.fs_num FROM 's3://financial-analysis-project-bucket/fs/{folder}/num.parquet' IAM_ROLE 'arn:aws:iam::207567756516:role/service-role/AmazonRedshift-CommandsAccessRole-20250321T104142' FORMAT AS PARQUET")
    
    cursor.execute(
        """
        INSERT INTO recorded (folder_name)
        VALUES (%s)
        """
        , folder
    )
            
    redshift_conn.commit()
    
    
def copy_to_postgres(folder):
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    engine = create_engine(f"postgresql+psycopg2://{postgreshook.connection.login}:{postgreshook.connection.password}@{postgreshook.connection.host}:{postgreshook.connection.port}/{postgreshook.connection.schema}")
    
    response = s3_hook.get_conn().get_object(Bucket="financial-analysis-project-bucket", Key=f"fs/{folder}/sub.parquet")
    data = response['Body'].read()
    table = pq.read_table(io.BytesIO(data))
    df = table.to_pandas()
    df.to_sql("fs_sub", engine, if_exists='append', index=False)

    response = s3_hook.get_conn().get_object(Bucket="financial-analysis-project-bucket", Key=f"fs/{folder}/num.parquet")
    data = response['Body'].read()
    table = pq.read_table(io.BytesIO(data))
    df = table.to_pandas()
    df.to_sql("fs_num", engine, if_exists='append', index=False)

    with engine.connect() as connection:
        connection.execute(text(
            """
            INSERT INTO recorded (folder_name)
            VALUES (:folder)
            """
            ),
            {"folder": folder}
        )
    
    print(f"Upload {folder} Success!")

default_args = {
    'owner': 'admin',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'fs_pipeline',
    default_args=default_args,
    description='pipeline for extracting finanical statements',
    schedule_interval=None,
    params={
        'connection_type': Param("postgres", enum=["postgres", "redshift"]),
        'folder': Param('2009q1', type="string")
    }
) as dag:
    
    task_branch1 = BranchPythonOperator(
        task_id='task_branch1',
        python_callable=branch1,
        op_kwargs={
            "folder": "{{ params.folder }}",
            "connection_type": "{{ params.connection_type }}",
        },
        dag=dag
    )
    
    task_branch2 = BranchPythonOperator(
        task_id='task_branch2',
        python_callable=branch2,
        op_kwargs={
            "connection_type": "{{ params.connection_type }}",
        },
        dag=dag
    )

    task_exit = BashOperator(
        task_id='task_exit',
        bash_command="echo Folder already in database"
    )
    
    task_get_companies = PythonOperator(
        task_id="task_get_companies",
        python_callable=get_companies
    )
    
    task_extract_fs = PythonOperator(
        task_id="task_extract_fs",
        python_callable=extract_fs,
        op_kwargs={
            "folder": "{{ params.folder }}",
        }
    )
    
    task_upload_to_s3 = PythonOperator(
        task_id="task_upload_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "folder": "{{ params.folder }}",
        }
    )
    
    task_copy_to_redshift = PythonOperator(
        task_id="task_copy_to_redshift",
        python_callable=copy_to_redshift,
        op_kwargs={
            "folder": "{{ params.folder }}",
        }
    )
    
    task_copy_to_postgres = PythonOperator(
        task_id="task_copy_to_postgres",
        python_callable=copy_to_postgres,
        op_kwargs={
            "folder": "{{ params.folder }}",
        }
    )
    
    task_branch1 >> [task_get_companies, task_exit] 
    task_get_companies >> task_extract_fs >> task_upload_to_s3 >> task_branch2
    task_branch2 >> [task_copy_to_redshift, task_copy_to_postgres]