import os
import re
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('.env')

AWS_ACCESS_KEY = os.getenv('AWS_PYTHON_USER_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_PYTHON_USER_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_BASE_FOLDER = os.getenv('S3_BASE_FOLDER')
LOCAL_FOLDER = 'data/raw'

s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

def file_exists_in_s3(bucket, prefix):
    """
        Checks if the specified file already exists on s3 folder or not.
    """
    print('Checking if file exists on s3...')
    response = s3_client.list_objects_v2(Bucket= bucket, Prefix=prefix, MaxKeys=1)
    return "Contents" in response

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
            if not (file.endswith('.parquet') or file.endswith('.csv')):
                continue
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, LOCAL_FOLDER)
            print('file-> ',file,'\nlocal path-> ', local_file_path, '\nrelative path-> ',relative_path)

            s3_key = f"{S3_BASE_FOLDER}/{relative_path}"
    
            # Check if file already exists
            if file_exists_in_s3(BUCKET_NAME, s3_key):
                print(f"🚫 File '{file}' already exists in S3. Skipping upload.\n")
                continue

            try:
                print(f"📤 Uploading {local_file_path} to s3://{BUCKET_NAME}/{s3_key} ...")
                s3_client.upload_file(local_file_path, BUCKET_NAME, s3_key)
                print(f"✅ Uploaded: {file}\n")
            except Exception as e:
                print(f"❌ Failed to upload {file}: {e}\n")

if __name__=="__main__":
    upload_files_to_s3()