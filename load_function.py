import boto3

from operations import load


s3_client = boto3.client('s3')
sqlite_bucket_name = "service3sqlbucket"

def lambda_handler(event, context):
    # Get bucket name and file key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    # Download the file from S3
    s3_client.download_file(bucket_name, file_key, '/tmp/' + file_key)
    
    db_file_path = load('/tmp/' + file_key)

    # Read SQLite DB and upload to S3
    with open(db_file_path, 'rb') as f:
        s3_client.put_object(Body=f, Bucket=sqlite_bucket_name, Key='sqlite_' + db_file_path)