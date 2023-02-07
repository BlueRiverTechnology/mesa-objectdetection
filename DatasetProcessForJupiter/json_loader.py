import json
import pandas as pd
with open('/home/patrikszepesi/Downloads/63d812995431c65c95cf11fb.jsonl') as json_file:
    json_list = list(json_file)

for json_str in json_list:
    result = json.loads(json_str)
    temp_df = pd.DataFrame(columns = ['id','artifact_id','s3_key','s3_bucket','kind'])
    #print(result['id'])
    id = result['id']
    artifacts = result['artifacts']
    for artifact in artifacts:
        s3_key_string = str(artifact['s3_key'])
        kind = str(artifact['kind'])
        if(s3_key_string.endswith(('png','jpg','jpeg'))) and kind == 'debayeredrgb':
            s3_key_string = s3_key_string
            kind = kind            
            bucket = str(artifact['s3_bucket'])
            artifact_id = str(artifact['id'])
            temp_df.loc[len(temp_df.index)] = [id,artifact_id,s3_key_string,bucket,kind]
            with open('Ben12.csv', 'a') as f:
                 temp_df.to_csv(f, header=False,index = False)
            break
            
    else: 
        for artifact in artifacts:
            s3_key_string = str(artifact['s3_key'])
            kind = str(artifact['kind'])
            if(s3_key_string.endswith(('png','jpg','jpeg'))) and kind == 'rgb':
                s3_key_string = s3_key_string
                kind = kind            
                bucket = str(artifact['s3_bucket'])
                artifact_id = str(artifact['id'])
                temp_df.loc[len(temp_df.index)] = [id,artifact_id,s3_key_string,bucket,kind]
                with open('Ben12.csv', 'a') as f:
                    temp_df.to_csv(f, header=False,index = False)
                break
        else:
            for artifact in artifacts:
                s3_key_string = str(artifact['s3_key'])
                kind = str(artifact['kind'])
                if(s3_key_string.endswith(('png','jpg','jpeg'))) and kind not in ('raw','pcd'):
                    s3_key_string = s3_key_string
                    kind = kind            
                    bucket = str(artifact['s3_bucket'])
                    artifact_id = str(artifact['id'])
                    temp_df.loc[len(temp_df.index)] = [id,artifact_id,s3_key_string,bucket,kind]
                    with open('Ben12.csv', 'a') as f:
                        temp_df.to_csv(f, header=False,index = False)
                    break
            else:
                print("no correct format found")
'''
import json
import pandas as pd
with open('63d45758bbb0a2587e721ecd.jsonl') as json_file:
    json_list = list(json_file)

for json_str in json_list:
    result = json.loads(json_str)
    temp_df = pd.DataFrame(columns = ['id','artifact_id','s3_key','s3_bucket'])
    #print(result['id'])
    id = result['id']
    artifacts = result['artifacts']
    for artifact in artifacts:
        s3_key_string = str(artifact['s3_key'])
        if(s3_key_string.endswith(('png','jpg','jpeg'))):            
            bucket = str(artifact['s3_bucket'])
            artifact_id = str(artifact['id'])
            temp_df.loc[len(temp_df.index)] = [id,artifact_id,s3_key_string,bucket]
            with open('Ben6.csv', 'a') as f:
                 temp_df.to_csv(f, header=False,index = False)
            

        break
'''
        #artifact_id = artifact['id']
        #print(artifact['s3_key'])
