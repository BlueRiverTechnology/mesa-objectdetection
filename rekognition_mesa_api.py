import time
import boto3 
import pandas as pd
import json
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from queue import Queue
import os



os.environ['BRT_ENV'] = 'dev'
import brtdevkit
brtdevkit.log = 'info'

# Use the AthenaClient to query the datalake
from brtdevkit.core.db.athena import AthenaClient
athena = AthenaClient()

# Create an Annotation
from brtdevkit.data.core import Annotation as AnnotationAPI

client = boto3.client('rekognition', region_name='us-west-2' ) #tartarus, mesa-data


label_map = {
        '1': 'Person',
        '2': 'Teen',
        '3': 'Boy',
        '4': 'Male',
        '5': 'Field'
        }
label_map_empty = {}


def rekognition():
            try:
                response = client.detect_labels(
                    Image={
                        #'Bytes': b'bytes',
                        'S3Object': {
                            'Bucket': 'mesa-data',
                            'Name': 'dev/images/fc258e60-b7dd-4e67-b612-106a98f45b84-datapoint_front-left-right_debayeredrgb.png',
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
                
                for i in range(total_detections):
                    rekognition_object = response['Labels'][i]['Name']
                    if len(response['Labels'][i]['Instances']) < 1 :
                        #style = 'bounding_box'
                        if(rekognition_object in label_map.values()):
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
                    elif len(response['Labels'][i]['Instances']) >= 1:
                        bounding_box_instances = len(response['Labels'][i]['Instances'])
                        style = 'bounding_box'
                        if(rekognition_object in label_map.values()):
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
                if style == 'categorical':
                    values = {
                    'kind': 'brt',#must be switched to rekognition
                    'project_name': 'jupiter',
                    'properties': properties,
                    'state': 'ok',
                    'style': style,
                    'vendor_metadata': {
                        'rekognition': {
                            'min_confidence': '0.6'
                            }
                        },
                    'label_map': label_map_empty,
                    #'s3_key': 'dev/images/3a9b9e2a-ae1e-411d-b485-f89c98b1d1ed-datapoint_front-center-right_debayeredrgb.png',
                    #'s3_bucket': 'mesa-data'
                        }

                elif style == 'bounding_box':
                    values = {
                    'kind': 'brt',#must be switched to rekognition
                    'project_name': 'jupiter',
                    'properties': properties,
                    'bounding_boxes': bounding_boxes,
                    'state': 'ok',
                    'style': style,
                    'vendor_metadata': {
                        'rekognition': {
                            'min_confidence': '0.6'
                            }
                        },
                    'label_map': label_map,
                    
                    #'s3_key': 'dev/images/3a9b9e2a-ae1e-411d-b485-f89c98b1d1ed-datapoint_front-center-right_debayeredrgb.png',
                    #'s3_bucket': 'mesa-data'
                        }
                annotation = AnnotationAPI(image_id=str('621d2ac2355efa33d3df5276'), values=values)
                #print(annotation)
                annotation.create()               
        
            except Exception as e:
                print('Error occured', e)
            
rekognition()
#That worked for bounding boxes
#checked ids: 621d27cd355efa33d3df4865
#checked keys: dev/images/3a9b9e2a-ae1e-411d-b485-f89c98b1d1ed-datapoint_front-center-right_debayeredrgb.png
#https://aletheia-dev.brtws.com/images/621d27cd355efa33d3df4865?project_name=jupiter&page=3


