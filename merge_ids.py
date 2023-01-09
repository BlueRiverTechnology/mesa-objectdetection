import pandas as pd 
import plotly.express as px


df = pd.concat(
    map(pd.read_csv,['time_rekognition_results_jupiter_unlabeled_100k_1.csv','time_rekognition_results_jupiter_unlabeled_100k_2.csv','time_rekognition_results_jupiter_unlabeled_100k_3.csv','time_rekognition_results_jupiter_unlabeled_100k_4.csv','time_rekognition_results_jupiter_unlabeled_100k_5.csv']) ,ignore_index = True)

#print(df.head())
fig = px.line(df, title= 'Rekognition Time')
fig.show()


df.to_csv('time.csv',index = False, header = False)