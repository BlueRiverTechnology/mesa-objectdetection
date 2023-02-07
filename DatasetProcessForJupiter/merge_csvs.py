import pandas as pd

df1 = pd.read_csv('rekognition_csv_results/rekognition_results_63d41719fb7638e4f60d72ea.csv')

df1['category'] = 'not yet measured'
df1['kind'] = 'not yet measured'

df2 = pd.read_csv('pre_final_rekognition_results_63d41719fb7638e4f60d72ea.csv')

d1 = df1['id'].nunique()
print(d1)

d2 = df2['id'].nunique()
print(d2)


print(d1+d2)


merged_df = pd.concat([df1,df2])
#merged_df = pd.merge(df1,df2, on='id')

merged_df.to_csv('final_rekognition_results_63d41719fb7638e4f60d72ea.csv',index = False)

d3 = merged_df['id'].nunique()
print(d3)

check=pd.read_csv('final_rekognition_results_63d41719fb7638e4f60d72ea.csv')

c = check['id'].nunique()
print(c)


