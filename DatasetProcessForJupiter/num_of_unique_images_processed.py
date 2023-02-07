import pandas as pd

df = pd.read_csv('final_rekognition_results_63d812995431c65c95cf11fb.csv')


u = df['id'].nunique()
print(u)


