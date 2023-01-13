import json
import base64
import time
import boto3
STREAM_NAME = "mesa.images.ingest"
client = boto3.client("kinesis", region_name="us-west-2")

def lambda_handler(event, context):
    print(event)
    for i in event['Records']:
        print("Raw data :",i['kinesis']['data']);
        print("base64 decoded data :",base64.b64decode(i['kinesis']['data']));
        print("UTF-8 decoded data :",base64.b64decode(i['kinesis']['data']).decode('utf-8'));
        
