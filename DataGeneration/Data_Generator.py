import json
import boto3

s3_client = boto3.client('s3')

# Upload the file to S3
s3_client.upload_file('transactions.txt', 'fh-data-insight', 'transactions.txt')
