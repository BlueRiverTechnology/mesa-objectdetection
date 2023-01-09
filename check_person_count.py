import pandas as pd
import json 


df = pd.read_csv('./rekognition_results_jupiter_unlabeled_100k_1.csv')



#get keys that have hasPerson = True that way we get how many unique keys we get
p_df = df[df['hasPerson'] == True]


df2 = p_df[p_df['confidence']>0.90]

df3 = df[(df['label'] == 'Person')]

df4 = df3[df3['confidence'] > 90]

#df4.to_csv('foo.csv',header = False)


#print(df4['id'].unique())
#print(len(df4['id'].unique()))

df5 = df4[['id','instances']]

#print(str(df5.values[0]))

#a = str(df5.values[14])


arr = []
for index, row in df5.iterrows():
    #print(str(row.values).count('BoundingBox'))
    arr.append(str(row.values).count('BoundingBox'))
    #df5['PersonCount'].loc = str(row.values).count('BoundingBox')


df5c = df5.copy()
df5c['personCount'] = arr



c = df5c['instances'].values


#for i in c:
 #   a = i.count('BoundingBox')
  #  for i in range(a):
   #     print()


#for index, element in enumerate(d):
   #pass

df5c.to_csv('PersonCount.csv',header = True, index = False )


new_df = pd.read_csv('PersonCount.csv')



new_df = new_df.drop('instances', axis =1)

print(new_df['personCount'].sum())


#print(new_df.head())

new_df.to_csv('final.csv')

