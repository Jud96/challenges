import pandas as pd

df = pd.read_csv('10299-finding-updated-records/data.csv')
df2 = df.groupby('id')['salary'].max().reset_index()
df2 = df2.merge(df, on=['id', 'salary'])
df2.rename(columns={'salary':'max'}, inplace=True)
df2.sort_values(by='id',ascending=True)