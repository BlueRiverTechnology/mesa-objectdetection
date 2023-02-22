import pandas as pd

df = pd.read_csv('final_rekognition_results_63ee7710bff1c7151e16b05e.csv')


u = df['id'].nunique()
print(u)


