import redshift_connector
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat_ws, lit, quarter, to_date, year
import boto3
import pyarrow.parquet as pq

# tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
# companies_cik = tickers['CIK'].to_list()
folder = "2010q1"

# spark = SparkSession.builder.getOrCreate()
# sub_df = spark.read.csv(f"./sub.txt", sep='\t', header=True, inferSchema=True)
# sub_df = sub_df.filter((sub_df.cik.isin(companies_cik)) & (sub_df.form.isin(["10-Q", "10-K"])))
# sub_df = sub_df.drop("zipba", "bas1", "bas2", "baph", "countryma", "stprma", "cityma", "zipma", "mas1", "mas2", "ein", "former", "changed", "afs", "wksi", "filed", "accepted", "prevrpt", "detail", "instance", "nciks", "aciks")
# sub_df = sub_df.withColumn("period", to_date("period", 'yyyyMMdd')).withColumn("folder", lit(folder))
# sub_df = sub_df.select([sub_df.columns[-1]] + sub_df.columns[:-1])
# sub_df.show()   
# sub_df.toPandas().to_parquet(f"./sub.parquet")

# num_df = spark.read.csv(f"./num.txt", sep='\t', header=True, inferSchema=True)        
# num_df = num_df.withColumn("ddate", to_date("ddate", 'yyyyMMdd'))        
# num_df = num_df.join(sub_df, (num_df["adsh"] == sub_df["adsh"]) & (year(num_df["ddate"]) == year(sub_df["period"])), "left_semi")
# num_df = num_df.drop("version", "coreg", "footnote") 
# num_df.show()
# num_df.toPandas().to_parquet(f"./num.parquet")

# table = pq.read_table('./num.parquet')
# print(table.schema)

# s3_client = boto3.client('s3')
# s3_client.upload_file(
#     "./num.parquet",
#     "financial-analysis-project-bucket",
#     "num.parquet"
# )

redshift_conn = redshift_connector.connect(
    host="default-workgroup.207567756516.us-east-1.redshift-serverless.amazonaws.com",
    database="dev",
    port=5439,
    user="admin",
    password="FYZVwhdhy002&!"
)
cursor = redshift_conn.cursor()

# cursor.execute(f"COPY dev.public.fs_num FROM 's3://financial-analysis-project-bucket/num.parquet' IAM_ROLE 'arn:aws:iam::207567756516:role/service-role/AmazonRedshift-CommandsAccessRole-20250321T104142' FORMAT AS PARQUET")
# redshift_conn.commit()

cursor.execute(
    """
    INSERT INTO recorded (folder_name)
    VALUES (%s)
    """,
    (folder,)
)      
redshift_conn.commit()
    
    
