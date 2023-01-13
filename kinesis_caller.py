import json
import datetime
import random
import boto3
client = boto3.client("kinesis", region_name="us-west-2")
STREAM_NAME = "mesa.images.ingest"


def getData(name, lowVal, highVal):
   data = {}
   data["name"] = name
   data["value"] = random.randint(lowVal, highVal) 
   return data

temp=0;

while temp<100:
   rnd = random.random()
   if (rnd < 0.01):
      data = json.dumps(getData("mesa-rekognition-fail", 100, 120))  
      client.put_record(StreamName=STREAM_NAME, Data=data, PartitionKey="1")
      print('*************************** anomaly *********************** ' + data)
   else:
      data = json.dumps(getData("mesa-rekognition-success", 10, 20))  
      client.put_record(StreamName=STREAM_NAME, Data=data, PartitionKey="1")
      print(data)
   temp=temp+1


