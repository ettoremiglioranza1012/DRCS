
"""
Ensure Required MinIO Buckets Exist

This utility script connects to a MinIO server (S3-compatible object storage) and 
ensures that all required buckets for the satellite wildfire detection pipeline 
are present. If any of the specified buckets are missing, it creates them.

Buckets checked/created:
    - satellite-imgs : for storing incoming raw satellite images (if used)
    - bronze         : raw JSON data layer
    - silver         : cleaned and structured Parquet data
    - gold           : aggregated and enriched analytics-ready data

Requirements:
    - boto3
    - MinIO running at endpoint http://minio:9000
    - Access credentials (minioadmin/minioadmin by default)

This script should be run once during system setup or service initialization.
"""

# Utilities
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
)

bucket_list = ["satellite-imgs", "bronze", "silver", "gold"]

buckets = s3.list_buckets().get('Buckets', [])
bucket_names = [b['Name'] for b in buckets]

for bucket_name in bucket_list:
    if bucket_name not in bucket_names:
        s3.create_bucket(Bucket=bucket_name)
        print(f"SUCCESS: Created bucket: {bucket_name}")
    else:
        print(f"Bucket {bucket_name} already exists.")
