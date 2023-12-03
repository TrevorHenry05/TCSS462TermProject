import boto3

from query import query

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    # Get bucket name and file key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    fil = event["Filters"] 
    ags = event["Group By"]

    fil.update({"Group By": ags})

   
    s3_client.download_file(bucket_name, 'sqlite_/tmp/'+ file_key, '/tmp/' + file_key)

    
    return query(requests=fil, db_file_path='/tmp/'+file_key)

    