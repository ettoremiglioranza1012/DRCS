import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
)

bucket_name = "bronze"

buckets = s3.list_buckets().get('Buckets', [])
bucket_names = [b['Name'] for b in buckets]

if bucket_name not in bucket_names:
    s3.create_bucket(Bucket=bucket_name)
    print(f"✅ Created bucket: {bucket_name}")
else:
    print(f"ℹ️  Bucket {bucket_name} already exists.")
