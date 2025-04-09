import boto3
import os
import zipfile

    
root = "./datasets/data"
s3_client = boto3.client('s3')

for year in range(2009, 2014+1):
    for quater in range(1, 4+1):
        extract_folder = os.path.join(root, f"{year}q{quater}")
        if not os.path.exists(extract_folder):
            os.makedirs(extract_folder)
            
        with zipfile.ZipFile(os.path.join("./datasets", f"{year}q{quater}.zip"), 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        
        print(f"Unzip {year}q{quater}.zip")        

for folder in os.listdir(root):
    for file in os.listdir(os.path.join(root, folder)):
        local_filename = os.path.join(root, folder, file)
        s3_filename = f"{folder}/{file}"
        s3_client.upload_file(local_filename, "financial-statement-datasets", s3_filename)
        print(f"Upload {local_filename} to {s3_filename}")
        