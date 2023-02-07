import pandas as pd


df = pd.read_csv('CsvNameToSaveTo.csv')

#column 0 = ids
#colum 1 = artifact id
#column 2 = s3 keys
#column 3 = bucket

ids = df.iloc[:,0]

print(len(ids)) #111430

unique_ids = set(ids)

print(len(unique_ids)) #111430

artifact_ids = df.iloc[:,1]

print(len(artifact_ids))#111430

unique_artifact_ids = set(artifact_ids)

print(len(unique_artifact_ids))#111430

s3_keys = df.iloc[:,2]

print(len(s3_keys)) #111430

unique_s3_keys = set(s3_keys)

print(len(unique_s3_keys)) #111430

s3_buckets = df.iloc[:,3]

print(len(s3_buckets))

unique_s3_buckets = set(s3_buckets)

print(len(unique_s3_buckets))#1

