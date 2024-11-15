import pandas as pd

fact_events = pd.read_csv('fact_events.csv')
fact_events['time_id'] = pd.to_datetime(fact_events['time_id'])
fact_events['month'] = fact_events['time_id'].dt.month

fact_events.groupby(['client_id', 'month']).agg({'user_id': 'nunique'}).reset_index()