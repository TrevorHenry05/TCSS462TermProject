import boto3
import json

from operations import load


s3_client = boto3.client('s3')
sqlite_bucket_name = "service3sqlbucket"

def lambda_handler(event, context):
    # Get bucket name and file key from the S3 event
    # bucket_name = event['Records'][0]['s3']['bucket']['name']
    # file_key = event['Records'][0]['s3']['object']['key']
    body = json.loads(event["body"])
    bucket_name = body["bucket_name"]
    file_key = body["key"]

    # Download the file from S3
    s3_client.download_file(bucket_name, file_key, '/tmp/' + file_key)
    
    db_file_path = load('/tmp/' + file_key)

    # Read SQLite DB and upload to S3
    key = "data.db"
    with open(db_file_path, 'rb') as f:
        s3_client.put_object(Body=f, Bucket=sqlite_bucket_name, Key=key)
        
    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": "application/json"
        },
        'body': json.dumps({
            'bucket_name': sqlite_bucket_name,
            'key': key
        })
    }