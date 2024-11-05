# Import your libraries
import pandas as pd

def rate(x):
    return x.count()*100.0/num_all_users
# Start writing code

df_all_users = pd.concat([facebook_friends.user1,facebook_friends.user2], axis=0)\
    .reset_index(name='user')
num_all_users = df_all_users['user'].nunique() 

df_reserved = facebook_friends[['user2','user1']]\
    .rename(columns={'user2':'user1','user1':'user2'})
# union df and df_reserved
df_all_connections = pd.concat([facebook_friends,df_reserved], axis=0).drop_duplicates()
# count the number of connections for each user /all users 9

df_all_connections_rate = df_all_connections.groupby('user1').agg({'user2':rate}).reset_index().rename(columns={'user2':'count'})
df_all_connections_rate