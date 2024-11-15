import pandas as pd

# Load the data

marketing_campaign = pd.read_csv('marketing_campaign.csv')
marketing_campaign['rnk1'] = marketing_campaign.groupby(['user_id'])['created_at']\
    .rank(method='dense')
marketing_campaign['rnk2'] = marketing_campaign\
    .groupby(['user_id', 'product_id'])['created_at']\
    .rank(method='dense')


len(marketing_campaign[marketing_campaign['rnk1'] > 1]
    [marketing_campaign['rnk2'] == 1]['user_id'].unique())

####################

df = pd.read_csv('data.csv')
df['created_at'] = pd.to_datetime(df['created_at'])
df['min_date'] = df.groupby('user_id')['created_at'].transform('min')
df['first_date'] = (df['created_at'] == df['min_date'])


first_purchases = df[df['first_date'] == True].groupby('user_id').agg(
    products_ids=('product_id', lambda x: list(x)),
).reset_index()

NFP = df[df['first_date'] == False]

# merge the two dataframes
df2 = pd.merge(first_purchases, NFP, on='user_id', how='inner')

# find rows that product_id is in products_ids
df2['purchased_new_in_future'] = df2.apply(lambda x: x['product_id']
                                           not in x['products_ids'], axis=1)
df2 = df2[df2['purchased_new_in_future'] == True]

df2['user_id'].nunique()
