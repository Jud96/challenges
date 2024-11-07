import pandas as pd

df = pd.read_csv("largest-olympics/data.csv")
df.groupby('games')['name'].nunique().reset_index()\
    .rename(columns={'name': 'athletes_count'})\
    .sort_values(by='athletes_count', ascending=False).head(1)
