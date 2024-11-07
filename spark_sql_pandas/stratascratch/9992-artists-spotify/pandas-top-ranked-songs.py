import pandas as pd

df = pd.read_csv("artists-spotify/data.csv")
df.head()
df[df['position'] == 1].groupby('trackname').size()\
    .reset_index(name='times_top1').sort_values('times_top1', ascending=False)