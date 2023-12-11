import boto3
import json
import logging

from query import query

s3_client = boto3.client('s3')

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

    fil = body["Filters"] 
    ags = body["Group By"]

    fil.update({"Group By": ags})

   
    s3_client.download_file(bucket_name, file_key, '/tmp/' + file_key)

    result = query(requests=fil, db_file_path='/tmp/'+file_key)
    logger.info(result)
    return result

    