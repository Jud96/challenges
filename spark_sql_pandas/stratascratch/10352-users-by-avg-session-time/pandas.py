# Import your libraries
import pandas as pd

facebook_web_log = pd.read_csv('facebook_web_log.csv')

facebook_web_log['date'] = pd.to_datetime(
    facebook_web_log['timestamp']).dt.date
df2 = facebook_web_log.pivot_table(index=['date', 'user_id'],
                                   columns='action',
                                   values=['timestamp'],
                                   aggfunc={'timestamp': ['max', 'min']})
temp = (pd.to_datetime(df2['timestamp']['min']['page_exit'])
        - pd.to_datetime(df2['timestamp']['max']['page_load'])).dt.total_seconds()
facebook_web_log = temp.groupby('user_id').mean().dropna()\
    .reset_index().rename(columns={0: 'duration'})
