import os
import re
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('.env')

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_BASE_FOLDER = os.getenv('S3_BASE_FOLDER')
LOCAL_FOLDER = 'data/raw'

s3_client = boto3.client('s3')

def folder_exists_in_s3(bucket, prefix):
    """
    Checks if a folder (prefix) exists in the S3 bucket.
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return "Contents" in response  # Returns True if folder exists, False otherwise

def create_folder_in_s3(bucket, prefix):
    """
    Creates an empty 'folder' (prefix) in S3 by uploading a dummy file.
    """
    s3_client.put_object(Bucket = bucket, Key = prefix)

def upload_files_to_s3():
    """
    Uploads all files from data/raw/YEAR to S3 under nyc-taxi-data-analytics/raw/YEAR
    """
    
    for root, _, files in os.walk(LOCAL_FOLDER):
        for file in files:
            if not file.endswith('.parquet'):
                continue
            local_file_path = os.path.join(root, file)

            # Use a regular expression to find the year
            match = re.search(r'_(\d{4})-', file)  # Pattern to find 'YYYY-' after an underscore
            if match:
                year = match.group(1)  # Extract the captured group (the year)
                print(match, year) 
            else:
                print("Year not found")
            
            s3_target_folder = f"{S3_BASE_FOLDER}/{year}"

            if not folder_exists_in_s3(BUCKET_NAME, s3_target_folder):
                print(f"📂 Folder '{s3_target_folder}' does not exist. Creating...")
                try:
                    create_folder_in_s3(BUCKET_NAME, s3_target_folder)
                    print(f"✅ Folder '{s3_target_folder}' created!\n")
                except Exception as e:
                    print(f"❌ Folder '{s3_target_folder}' not created due to : {e}\n")
            
            s3_key = f"{s3_target_folder}/{file}"

            try:
                print(f"📤 Uploading {local_file_path} to s3://{BUCKET_NAME}/{s3_key} ...")
                s3_client.upload_file(local_file_path, BUCKET_NAME, s3_key)
                print(f"✅ Uploaded: {file}\n")
            except Exception as e:
                print(f"❌ Failed to upload {file}: {e}\n")

if __name__=="__main__":
    upload_files_to_s3()

            

            
