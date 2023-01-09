import time
import boto3 
import pandas as pd
import json
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from queue import Queue




#REPLACE WITH YOUR DESIRED REGION
client = boto3.client('rekognition', region_name='us-west-2' ) #tartarus, mesa-data

#REPLACE WITH THE FILE YOU WANT TO SUBMIT TO REKOGNITION
file_df = pd.read_csv('jupiter_unlabeled_100k_5.csv')#[:5]
#convert df to array
array = file_df.values

#FINAL CSV, WHERE WE WILL APPEND REKOGNITION OUTPUT TO
csv_name = 'rekognition_results_jupiter_unlabeled_100k_5.csv'

#init empty df to add data to
df = pd.DataFrame(columns=[ 'index','id', 'date', 'camera_location', 'script_id', 'web_s3_bucket', 'web_s3_key', 'label', 'confidence','instances', 'hasPerson', 'hasCar'], dtype = object)

df.to_csv( csv_name, index = False)

lock = threading.Lock()

#store ids of images we have already used
id_set = set()


parser = argparse.ArgumentParser()
parser.add_argument('--check_prev_ids', type = str, required = False)
args = parser.parse_args()

if(args.check_prev_ids == 'yes'):
    #REPLACE WITH CSV YOU WANT TO CHECK IDS Tids_parllel_rekognition_results_tartarus_jupiter_humans_54K.csvO
    ids_to_check_df = pd.read_csv('./ids_parllel_rekognition_results_tartarus_jupiter_humans_54K.csv')
    ids_array = ids_to_check_df.values
elif(args.check_prev_ids == 'no'):
    ids_array = []

#Queue init to time rekognition response
q = Queue()

def rekognition(row):
    
    
    #We can do this because the elements in a python list are persistent
    #   0      1    2        3              4           5          6        7        
    #  index, id, date,camera_location, script_id,web_s3_bucket,web_s3_key, year

    if(row[1] in ids_array):
       print(f'alredy proccessed{row[1]}')
    elif(row[1] not in ids_array): 
        if(row[5] == 's3.aletheia.imports.prod'):
            print('Not processing, because of differnet region')      
        elif(row[5] == 'tartarus.images' or 'mesa-data'):
            try:
                t1_start = perf_counter()
                response = client.detect_labels(
                    Image={
                        #'Bytes': b'bytes',
                        'S3Object': {
                            'Bucket': row[5],
                            'Name': row[6],
                            #'Version': 'string'
                        }
                    },
                    MaxLabels=30,
                    MinConfidence=60
                )
                t1_stop = perf_counter()
                q.put(t1_stop-t1_start)
                length_of_labels = len(response['Labels'])
                temp_df = pd.DataFrame(columns=['index', 'id', 'date', 'camera_location', 'script_id', 'web_s3_bucket', 'web_s3_key', 'label', 'confidence','instances', 'hasPerson', 'hasCar'], dtype = object)
                for i in range(length_of_labels):
                    temp_df.loc[len(temp_df.index)] = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], response['Labels'][i]['Name'] , response['Labels'][i]['Confidence'], response['Labels'][i]['Instances'], response['Labels'][i]['Name'] == 'Person', response['Labels'][i]['Name'] == 'Car']
        
                with lock:
                    with open(csv_name, 'a') as f:
                        temp_df.to_csv(f, header=False,index = False)
                id_set.add(row[1])
        
        
            except Exception as e:
                print('Error occured', e)
            
        

start = time.time()

with ThreadPoolExecutor(50) as executor:
    executor.map( rekognition, array )

        
#create csv from ids we have looped over     
ids_list = list(id_set)
ids_df = pd.DataFrame(ids_list)
ids_df.to_csv(f'ids_{csv_name}',index=False)

lst = []

for i in q.queue:
    lst.append(i)



total_time = pd.Series(lst)



df = pd.DataFrame({'total time':total_time})

df.to_csv(f'time_{csv_name}', header = False,index = False)
end = time.time()
   

print(f'It took {end-start} seconds to run')