import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
from sqlalchemy import create_engine
import time

# s3_client = boto3.client('s3')

# Connection details
user = 'airflow'
password = 'airflow'
host = 'localhost'
port = '5432'
database = 'financial_statements'

# Create the connection string
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

df = pd.read_parquet('./stock.parquet')
df.to_sql("stock", engine, if_exists='replace', index=False)


# folders = ['2024q2']

# num_key_list = [f"{file}/num.txt" for file in folders]
# sub_key_list = [f"{file}/sub.txt" for file in folders]

# for num_file in num_key_list:
#     start_time = time.time()
#     response = s3_client.get_object(Bucket="financial-statement-datasets", Key=num_file)
#     read_time = time.time() - start_time
#     print(read_time)
#     data = response['Body'].read()
#     table = pq.read_table(io.BytesIO(data))
    # df = table.to_pandas()
    # print(df.head())
    # df.head(0).to_sql('fs_num', engine, if_exists='replace', index=False) # create empty table
    # df.to_sql('fs_num', engine, if_exists='append', index=False)
    
# for sub_file in sub_key_list:
#     response = s3_client.get_object(Bucket="financial-analysis-project-bucket", Key=sub_file)
#     data = response['Body'].read()
#     table = pq.read_table(io.BytesIO(data))
#     df = table.to_pandas()
#     print(df.head())
#     # df.head(0).to_sql('fs_sub', engine, if_exists='replace', index=False) # create empty table
#     # df.to_sql('fs_sub', engine, if_exists='append', index=False)
    