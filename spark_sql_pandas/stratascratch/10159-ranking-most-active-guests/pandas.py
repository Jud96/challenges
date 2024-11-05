import  pandas as pd

df = pd.read_csv('10159-ranking-most-active-guests/data.csv') 
df = df.groupby('id_guest')['n_messages'].sum().reset_index()
df['ranking'] = df['n_messages'].rank(method='dense', ascending=False)
df.sort_values('ranking', inplace=True)