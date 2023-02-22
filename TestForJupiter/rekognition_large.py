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
file_df = pd.read_csv('Ben17.csv')
#convert df to array
array = file_df.values

#FINAL CSV, WHERE WE WILL APPEND REKOGNITION OUTPUT TO
csv_name = f'final_rekognition_results_63ee7710bff1c7151e16b05e.csv'

#init empty df to add data to
df = pd.DataFrame(columns=['id', 'artifact id', 's3_bucket' , 's3_key', 'bounding box' ,'bounding box confidence', 'classification', 'classification confidence','category','kind'], dtype = object)

df.to_csv( csv_name, index = False)

lock = threading.Lock()

#store ids of images we have already used
#id_set = set()




filtered_classes = ["Person","Adult","Male","Female","Boy","Girl","Baby","Child","Fallen person","Utility Pole","Road Sign",
"Fence","Post","Pump","Pipe","Water","Pond","Stream","Drain","Building","House","Barn","Road","Animal","Bird","Dog","Insect",
"Cat","Cow","Sheep","Car","Vehicle","Truck","Tractor","Farm plow","Machine","Bulldozer","Bicycle","Motorcycle","Moving Van",
"Pickup Truck","Trash","Plastic","Plastic Bag","Garbage","Box","Package","Bag","Shopping Bag","Tote Bag","Tree","Tree Trunk",
"Tree Stump","Palm Tree","Bush","Weed","Rain","Fog","Night","Cloud","Snow","Pollution","Smoke","Smog"]

filtered_categories = []




parser = argparse.ArgumentParser()
parser.add_argument('--check_prev_ids', type = str, required = False)
args = parser.parse_args()

if(args.check_prev_ids == 'yes'):
    #REPLACE WITH CSV YOU WANT TO CHECK IDS Tids_parllel_rekognition_results_tartarus_jupiter_humans_54K.csvO
    ids_to_check_df = pd.read_csv('rekognition_csv_results/artifacts2.csv')
    ids_array = ids_to_check_df.values
elif(args.check_prev_ids == 'no'):
    ids_array = []

q = Queue()
#image_error_q = Queue()
def rekognition(row):
    id = row[0]
    artifact_id = row[1]
    s3_key = row[2]
    s3_bucket =row[3]
    kind = row[4]
    
    if(artifact_id in ids_array):
       #print(f'alredy proccessed{id}')
       return
    elif(id not in ids_array): 
        if(s3_bucket == 's3.aletheia.imports.prod'):
            #print('Not processing, because of permission issues')  
            return    
        if(s3_bucket == 'tartarus.images' or 'mesa-data'):
            try:
                t1_start = perf_counter()
                response = client.detect_labels(
                    Image={
                        #'Bytes': b'bytes',
                        'S3Object': {
                            'Bucket': s3_bucket,
                            'Name': s3_key,
                            #'Version': 'string'
                        }
                    },
                    MaxLabels=30,
                    MinConfidence=60
                )
                t1_stop = perf_counter()
                q.put(t1_stop-t1_start)
                length_of_labels = len(response['Labels'])
                temp_df = pd.DataFrame(columns=['id', 'artifact id','s3_bucket' , 's3_key', 'bounding box' ,'bounding box confidence', 'classification', 'classification confidence', 'category','kind'], dtype = object)
                for i in range(length_of_labels):
                    if(response['Labels'][i]['Name'] in filtered_classes):
                        bounding_box_instances = len(response['Labels'][i]['Instances'])
                        confidence = response['Labels'][i]['Confidence']
                        #print(bounding_box_instances)
                        if bounding_box_instances > 0:
                            for j in range(bounding_box_instances):
                                bounding_box_path = response['Labels'][i]['Instances'][j]['BoundingBox']
                                bound_box_confidence = response['Labels'][i]['Instances'][j]['Confidence']
                                #print(bounding_box_path,bound_box_confidence)
                                temp_df.loc[len(temp_df.index)] = [id, artifact_id, s3_bucket, s3_key, {'bounding box':bounding_box_path}, {'bounding box confidence':bound_box_confidence},{'classification':response['Labels'][i]['Name']}, {'confidence':confidence},response['Labels'][i]['Categories'][0]['Name'],kind]
                        elif bounding_box_instances < 1:
                            confidence = response['Labels'][i]['Confidence']

                            temp_df.loc[len(temp_df.index)] = [id, artifact_id, s3_bucket, s3_key, {'bounding box': 'none'}, {'bounding box confidence': 'none'},{'classification':response['Labels'][i]['Name']}, {'classification confidence': confidence},response['Labels'][i]['Categories'][0]['Name'],kind]
        
                with lock:
                    with open(csv_name, 'a') as f:
                        temp_df.to_csv(f, header=False,index = False)
                #id_set.add(id)
        
        
            except Exception as e:
                print('Error occured', e)
                #image_error_q.put(row)

start = time.time()

with ThreadPoolExecutor(30) as executor:
    executor.map( rekognition, array )

        
#create csv from ids we have looped over     
#ids_list = list(id_set)
#ids_df = pd.DataFrame(ids_list)
#ids_df.to_csv(f'ids_{csv_name}',index=False)

lst = []

for i in q.queue:
    lst.append(i)


#for i in image_error_q.queue:
 #   print(i,'QUEEEEEEEEE')

total_time = pd.Series(lst)


#differences = total_time.diff().fillna(total_time)

df = pd.DataFrame({'total time':total_time})
#df = pd.DataFrame(columns = ['A',],data=[a])

df.to_csv(f'time_{csv_name}', header = False,index = False)
end = time.time()
   
#print(file_df[file_df['web_s3_bucket']== 's3.aletheia.imports.prod'])

print(f'It took {end-start} seconds to run')