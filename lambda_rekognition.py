

import urllib
import json
import base64
import time
import boto3


import pandas as pd
import numpy as np
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from queue import Queue
from botocore.exceptions import ClientError


rekognition_client = boto3.client('rekognition', region_name='us-west-2' ) #tartarus, mesa-data

client = boto3.client("kinesis", region_name="us-west-2")







#TODO:
#Labelmap

label_map = {
        '1': 'Person',
        '2': 'Teen',
        '3': 'Boy',
        '4': 'Male',
        '5': 'Field'
        }
label_map_empty = {}

def lambda_handler(event, context):
    api_key = get_secret()
    #print(event,"THIS WAS THE EVENT")
    for i in event['Records']:
    #for i in range(1):
        #print("Raw data :",i['kinesis']['data']);
        #print("base64 decoded data :",base64.b64decode(i['kinesis']['data']));
        #print("UTF-8 decoded data :",base64.b64decode(i['kinesis']['data']).decode('utf-8'));
        
        
        rekogntion_region_1 = boto3.client('rekognition', region_name='us-west-1' ) #s3.aletheia.imports.prod
        rekogntion_region_2 = boto3.client('rekognition', region_name='us-west-2' ) #tartarus, mesa-data
        #break has to align with print statement print(s3_key_string) #do rekognition
        data = base64.b64decode(i['kinesis']['data'])
        json_data_array = []
        json_data = [json.loads(data)]
        for i in json_data:
            json_data_array.append(i)
        #print(json_data,'json dataaa')
        print(json_data_array,'data arrrrraaay')
        def rekognition(item):
            print(item,'ITEEEEEEEEM')
            try:
                image_id = item['image_id]
                for i in item['artifacts']:
                
                    s3_key_string = str(i['s3_key'])
                    if(s3_key_string.endswith(('png','jpg','jpeg'))):
                    
                        #print(s3_key_string) #do rekognition
                        s3_key = i['s3_key']
                        s3_bucket =i['s3_bucket']
                        region = i['region']
                        if region == 'us-west-1':
                            rekogntion_client = rekogntion_region_1
                        elif region == 'us-west-2':
                            rekognition_client = rekogntion_region_2
                        response = rekognition_client.detect_labels(
                            Image={
                                #'Bytes': b'bytes',
                                'S3Object': {
                                    'Bucket': s3_bucket,
                                    'Name': s3_key
                                    #'Version': 'string'
                                    }
                                },
                            MaxLabels=30,
                            MinConfidence=60
                            )
                        total_detections = len(response['Labels'])
                        properties = []
                        style = 'categorical'
                        bounding_boxes = []
                        total_relevant_classifications = 0
                        total_relevant_detections = 0
                        for i in range(total_detections):
                        
                        
                            rekognition_object = response['Labels'][i]['Name']
                            if len(response['Labels'][i]['Instances']) < 1 :
                                style = 'categorical'
                                if(rekognition_object in label_map.values()):
                                    total_relevant_classifications=+1
                                    properties.append(
                                    {
                                        'title': rekognition_object ,
                                        'value': rekognition_object,
                                        'confidence': response['Labels'][i]['Confidence']/100,
                                            'answer': {
                                                'title': 'yes',
                                                'value': 'yes' 
                                        }
                                })
                            #for bounding boxes
                            elif len(response['Labels'][i]['Instances']) >= 1 and rekognition_object in label_map.values():
                                bounding_box_instances = len(response['Labels'][i]['Instances'])
                                style = 'bounding_box'
                                print(style)
                                total_relevant_detections +=1
                                properties.append(
                                    {
                                        'title': rekognition_object ,
                                        'value': rekognition_object,
                                        'confidence': response['Labels'][i]['Confidence']/100,
                                            'answer': {
                                                'title': 'yes',
                                                'value': 'yes'
                                                    }
                                    })
                        
                                for j in range(bounding_box_instances):
                                    bounding_box_path = response['Labels'][i]['Instances'][j]['BoundingBox']
                                    bound_box_confidence = response['Labels'][i]['Instances'][j]['Confidence']
                                
                            
                                    #bounding box coordinates
                                    bottom_right_x = bounding_box_path['Left'] + bounding_box_path['Width']
                                    bottom_right_y = bounding_box_path['Top'] + bounding_box_path['Height']
                                    top_left_x = bounding_box_path['Left']
                                    top_left_y = bounding_box_path['Top']
                                    #confidence had to be scaled
                                    #kind has to be allowed to be rekognition
                          
                                    bounding_boxes.append(
                                        {
                                            'bottom_right': [
                                                int(bottom_right_x),
                                                int(bottom_right_y)
                                            ],
                                            'confidence': float(bound_box_confidence/100),#check
                                            'data_class': int([k for k, v in label_map.items() if v == rekognition_object][0]),
                                            #'data_class': label_map[rekognition_object],
                                            'top_left': [
                                                int(top_left_x),
                                                int(top_left_y)
                                            ]
                                        }
                                    )
                        if  total_relevant_classifications !=0 and total_relevant_detections == 0:
                                values = {
                                'kind': 'rekognition',#must be switched to rekognition
                                'project_name': 'jupiter',
                                'properties': properties,
                                'state': 'ok',
                                'style': 'categorical',
                                'vendor_metadata': {
                                    'rekognition': {
                                        'min_confidence': '0.6'
                                        }
                                    },
                                'label_map': label_map_empty,
                                #'s3_key': 'dev/images/3a9b9e2a-ae1e-411d-b485-f89c98b1d1ed-datapoint_front-center-right_debayeredrgb.png',
                                #'s3_bucket': 'mesa-data'
                                    }

                        elif  total_relevant_detections !=0:
                                values = {
                                'kind': 'rekognition',#must be switched to rekognition
                                'project_name': 'jupiter',
                                'properties': properties,
                                'bounding_boxes': bounding_boxes,
                                'state': 'ok',
                                'style': 'bounding_box',
                                'vendor_metadata': {
                                    'rekognition': {
                                        'min_confidence': '0.6'
                                        }
                                    },
                                'label_map': label_map,
                    
                                #'s3_key': 'dev/images/3a9b9e2a-ae1e-411d-b485-f89c98b1d1ed-datapoint_front-center-right_debayeredrgb.png',
                                #'s3_bucket': 'mesa-data'
                                }
                        elif total_relevant_detections == 0 and total_relevant_classifications == 0:
                                values = {
                                'kind': 'rekognition',#must be switched to rekognition
                                'project_name': 'jupiter',
                                'properties': [],
                                'state': 'ok',
                                'style': 'bounding_box',
                                'nothing_to_annotate': True,
                                'vendor_metadata': {
                                    'rekognition': {
                                        'min_confidence': '0.6'
                                        }
                                    },
                                'label_map': label_map,
                    
                                #'s3_key': 'dev/images/3a9b9e2a-ae1e-411d-b485-f89c98b1d1ed-datapoint_front-center-right_debayeredrgb.png',
                                #'s3_bucket': 'mesa-data'
                                }
                       
                        print(values,'VALUES')
                        
                        #annotation.create(values)
                        
                        try:
                            url = f'https://tartarus-dev-api.brtws.com/annotation/{image_id}'
                            data = {values}
                            data = json.dumps(data).encode()
                            req = urllib.request.Request(url, data = data, method = 'POST')
                            req.add_header('Content-Type', 'application/json')
                            response = urllib.request.urlopen(req)
                            print(response.read().decode())
                        except urllib.error.HTTPError as err:
                            print(f'error occured:  {err}')
                    break
                    
                    
            except Exception as e:
                print('Error occured', e)
                #PUT MESSAGE TO QUEUE
                
        


        with ThreadPoolExecutor(50) as executor:
            executor.map( rekognition, json_data_array )
            
        #call kinesis with failed
        #STREAM_NAME = "mesa.image.objectdetection.failed"
        #try:
            #data = bytes(str(json_array).encode("utf-8"))
            #data_dict = json_array
            #data = json.dumps(data_dict, indent=2).encode('utf-8')
            #print(f"Sending {data=}")
            #response = client.put_records(Records =[{'Data':data, 'PartitionKey':'A'}],StreamName=STREAM_NAME)
            #response = client.put_record(StreamName=STREAM_NAME, Data =data, PartitionKey = 'A')

            #print(f"Received {response=}")
        #except KeyboardInterrupt:
            #print("Unexpected error")import boto3


def get_secret():

    secret_name = "API_Key"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    json_secret = json.loads(secret)
    value = json_secret['API_Key']
    
    return value


#GET REQUEST TO PROD API
#def lambda_handler(event, context):
 #   api_key = get_secret()
  #  url = 'https://tartarus-dev-api.brtws.com/images'
   # params = {'count': 10, 'page': 1, 'project_name': 'jupiter'}
    #query_string = urllib.parse.urlencode(params)
    #headers = {'Accept': 'application/json', 'API-Key': api_key}    
    #print(headers)
    #req = urllib.request.Request(url + '?' + query_string, headers=headers)
    #try:
     #   response = urllib.request.urlopen(req)
      #  data = response.read()
       # return {
        #    'statusCode': response.status,
         #   'body': data.decode("utf-8")
        #}
    #except urllib.error.HTTPError as err:
     #   return {
      #      'statusCode': err.code,
       #     'body': err.reason
        #}
    
