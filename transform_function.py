import boto3
import json
import logging

from operations import transform


s3_client = boto3.client('s3')
transformed_csv_bucket_name = "service2csvbucket"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Get bucket name and file key from the S3 event
    # bucket_name = event['Records'][0]['s3']['bucket']['name']
    # file_key = event['Records'][0]['s3']['object']['key']
    logger.info(event)
    body = json.loads(event["body"])
    bucket_name = body["bucket_name"]
    file_key = body["key"]

    # Download the file from S3
    s3_client.download_file(bucket_name, file_key, '/tmp/' + file_key)

    transformed_file_path = transform('/tmp/' + file_key)

    # Read the transformed data and upload to S3
    key = 'transformed_' + file_key
    with open(transformed_file_path, 'rb') as f:
        s3_client.put_object(Body=f, Bucket=transformed_csv_bucket_name, Key=key)
        
    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": "application/json"
        },
        'body': json.dumps({
            'bucket_name': transformed_csv_bucket_name,
            'key': key
        })
    }