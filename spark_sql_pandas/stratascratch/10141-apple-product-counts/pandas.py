import pandas as pd

events = pd.read_csv('10141-apple-product-counts/playbook_events.csv')
users = pd.read_csv('10141-apple-product-counts/playbook_users.csv')

df_merged = events.merge(users, on='user_id', how='inner')
df_all_users_agg_lang = df_merged.groupby('language').agg(
    {'user_id': 'nunique'}).reset_index().rename(columns={'user_id': 'n_total_users'})

df_all_users_iphone_lang = df_merged[
    df_merged.device.isin(['macbook pro', 'iphone 5s', 'ipad air'])]\
    .groupby('language').agg({'user_id': 'nunique'})\
    .reset_index().rename(columns={'user_id': 'n_apple_users'})

df_all_users_agg_lang.merge(df_all_users_iphone_lang,
                            on='language', how='outer').fillna(
    0).sort_values('n_total_users', ascending=False)
