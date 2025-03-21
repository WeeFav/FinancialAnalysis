import os
import boto3

s3_client = boto3.client('s3')

response = s3_client.list_buckets()
for b in response['Buckets']:
    print(b)