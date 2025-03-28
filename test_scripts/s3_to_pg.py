import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
from sqlalchemy import create_engine

s3_client = boto3.client('s3')

# Connection details
user = 'postgres'
password = 'marvin0807'
host = 'localhost'
port = '5432'
database = 'financial_analysis'

# Create the connection string
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

folders = ['2024q2']

num_key_list = [f"fs/{file}/num.parquet" for file in folders]
sub_key_list = [f"fs/{file}/sub.parquet" for file in folders]

for num_file in num_key_list:
    response = s3_client.get_object(Bucket="financial-analysis-project-bucket", Key=num_file)
    data = response['Body'].read()
    table = pq.read_table(io.BytesIO(data))
    df = table.to_pandas()
    df.to_sql('fs_num', engine, if_exists='replace', index=False)
    
for sub_file in sub_key_list:
    response = s3_client.get_object(Bucket="financial-analysis-project-bucket", Key=sub_file)
    data = response['Body'].read()
    table = pq.read_table(io.BytesIO(data))
    df = table.to_pandas()
    df.to_sql('fs_sub', engine, if_exists='replace', index=False)
    