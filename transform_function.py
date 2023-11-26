import boto3
import csv
import os
from datetime import datetime

s3_client = boto3.client('s3')
transformed_csv_bucket_name = "service2csvbucket"

def lambda_handler(event, context):
    # Get bucket name and file key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    # Download the file from S3
    s3_client.download_file(bucket_name, file_key, '/tmp/' + file_key)

    # Get the path of the transformed CSV
    transformed_file_path = transform_csv('/tmp/' + file_key)

    # Read the transformed data and upload to S3
    with open(transformed_file_path, 'rb') as f:
        s3_client.put_object(Body=f, Bucket=transformed_csv_bucket_name, Key='transformed_' + file_key)

def transform_csv(file_path):
    transformed_rows = []
    seen_order_ids = set()
    with open(file_path, newline='', mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Check if the row is unique
            if row['Order ID'] in seen_order_ids:
                continue
            
            # Add the order id to the seen order id list
            seen_order_ids.add(row['Order ID'])
            
            # Calculate Order Processing Time
            order_date = datetime.strptime(row['Order Date'], '%m/%d/%Y')
            ship_date = datetime.strptime(row['Ship Date'], '%m/%d/%Y')
            row['Order Processing Time'] = (ship_date - order_date).days

            # Transform Order Priority
            order_priority_mapping = {'H': 'High', 'C': 'Critical', 'L': 'Low', 'M': 'Medium'}
            row['Order Priority'] = order_priority_mapping.get(row['Order Priority'])

            # Calculate Gross Margin
            total_profit = float(row['Total Profit'])
            total_revenue = float(row['Total Revenue'])
            row['Gross Margin'] = total_profit / total_revenue if total_revenue else 0

            transformed_rows.append(row)

    # Write the transformed data to a new CSV filew
    transformed_file_path = '/tmp/transformed_' + os.path.basename(file_path)
    with open(transformed_file_path, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames + ['Order Processing Time', 'Gross Margin'])
        writer.writeheader()
        writer.writerows(transformed_rows)
    
    return transformed_file_path