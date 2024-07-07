import boto3
import os
import time

# Define the AWS S3 bucket name and file details
BUCKET_NAME = 'test-autoloder-bucket'
SOURCE_DIRECTORY = '/Users/patel/Desktop/new_project_autoloder/datasets/101_json'
S3_PREFIX = 'growth_data/incoming_data/'

# Set up AWS credentials
os.environ['AWS_ACCESS_KEY_ID'] = '*****************************************'
os.environ['AWS_SECRET_ACCESS_KEY'] = '*****************************************'

# Initialize the S3 client
s3 = boto3.client('s3')


# Function to upload a file to S3
def upload_to_s3(file_path, bucket_name, s3_key):
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"Successfully uploaded {file_path} to {bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading file {file_path}: {e}")


# Function to sequentially upload files from a directory to S3
def copy_files_to_s3(source_directory, bucket_name, s3_prefix, delay=5):
    for file_name in os.listdir(source_directory):
        # Skip hidden files
        if file_name.startswith('.'):
            continue

        file_path = os.path.join(source_directory, file_name)
        if os.path.isfile(file_path):
            s3_key = os.path.join(s3_prefix, file_name)
            upload_to_s3(file_path, bucket_name, s3_key)
            time.sleep(delay)


# Call the function to start uploading files
copy_files_to_s3(SOURCE_DIRECTORY, BUCKET_NAME, S3_PREFIX)
