# Import your libraries
import pandas as pd

# Start writing code
fact_events = pd.read_csv('fact_events.csv')
fact_events['video_voice_event'] = fact_events['event_type']\
    .isin(['video call received',
           'video call sent',
           'voice call received', 'voice call sent'])

df2 = fact_events.groupby(['client_id', 'user_id']).agg(
    envent_cnt=('event_type', 'count'),
    video_voice_event=('video_voice_event', 'sum')
).reset_index()
df2['pct_video_voice_event'] = df2['video_voice_event']*100 / df2['envent_cnt']
result = df2[df2['pct_video_voice_event'] >= 50].groupby('client_id').size(
).reset_index(name='count').sort_values('count', ascending=False).head(10)
result[['client_id']].head(1)
