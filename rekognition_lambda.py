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

STREAM_NAME = "mesa.images.ingest" #Kinesis
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
    #print(event)
    for i in event['Records']:
    #for i in range(1):
        #print("Raw data :",i['kinesis']['data']);
        #print("base64 decoded data :",base64.b64decode(i['kinesis']['data']));
        #print("UTF-8 decoded data :",base64.b64decode(i['kinesis']['data']).decode('utf-8'));
        
        
        rekogntion_region_1 = boto3.client('rekognition', region_name='us-west-1' ) #s3.aletheia.imports.prod
        rekogntion_region_2 = boto3.client('rekognition', region_name='us-west-2' ) #tartarus, mesa-data
        #break has to align with print statement print(s3_key_string) #do rekognition
        
        data = base64.b64decode(i['kinesis']['data'])
        json_data = json.loads(data)
        def rekognition(item):
            try:
                
                for i in item['artifacts']:
                    s3_key_string = str(i['s3_key'])
                    if(s3_key_string.endswith(('png','jpg','jpeg'))):
                        print(s3_key_string) #do rekognition
                        s3_key = item['artifacts'][i]['s3_key']
                        s3_bucket = item['artifacts'][i]['s3_bucket']
                        region = item['artifacts'][i]['region']
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
                        print(response)
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
                                print('meeeee')
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
                            
                            #print(values)
                        break
                    
                    
            except Exception as e:
                print('Error occured', e)
                #PUT MESSAGE TO QUEUE
                
        
                
                

        with ThreadPoolExecutor(50) as executor:
            executor.map( rekognition, json_data )
            
        #call kinesis with failed