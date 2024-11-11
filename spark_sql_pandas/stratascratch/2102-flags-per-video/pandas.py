import pandas as pd

user_flags = pd.read_csv('data/user_flags.csv')
user_flags = user_flags[user_flags['flag_id'].notnull()]

user_flags['full_name'] = user_flags['user_firstname'].astype(str) +  user_flags['user_lastname'].astype(str)

user_flags .groupby('video_id').agg({'full_name': 'nunique'}).reset_index()\
    .rename(columns={'full_name': 'num_unique_users'})[['video_id', 'num_unique_users']]