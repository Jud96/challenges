import pandas as pd

user_flags = pd.read_csv('user_flags.csv')

df = user_flags[user_flags['flag_id'].notnull()]\
    .groupby(['user_firstname', 'user_lastname'])\
    .agg({'video_id': 'nunique'}).reset_index().rename(columns={'video_id': 'approved_flags'})


max_flags = df['approved_flags'].max()

df = df[df['approved_flags'] == max_flags]
df['full_name'] = df['user_firstname'] + ' ' + df['user_lastname']
df[['full_name']]
