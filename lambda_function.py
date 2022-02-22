import json
import io
import os
from io import StringIO
import boto3
import pandas as pd


def lambda_handler(event, context):
    # TODO implement
    # print(event)
    key1 = os.environ['Access_key']
    key2 = os.environ['Secret_access_key']

    s3_file_key = event['Records'][0]['s3']['object']['key'];
    bucket = event['Records'][0]['s3']['bucket']['name'];

    print(bucket)
    print(s3_file_key)

    s3 = boto3.client('s3', aws_access_key_id=key1, aws_secret_access_key=key2)

    obj = s3.get_object(Bucket=bucket, Key=s3_file_key)

    df = pd.read_csv(io.BytesIO(obj['Body'].read()));

    print(df)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


