import pandas as  pd


df = pd.read_csv('time_rekognition_results_jupiter_unlabeled_100k_1.csv')

print(df[1:].mean())