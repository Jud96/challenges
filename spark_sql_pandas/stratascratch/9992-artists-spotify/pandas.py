import pandas as pd

df = pd.read_csv("artists-spotify/data.csv")
df.head()
df.groupby('artist').size().reset_index(name='n_occurences')\
    .sort_values('n_occurences', ascending=False)