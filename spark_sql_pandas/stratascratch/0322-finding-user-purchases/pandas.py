# Import your libraries
import pandas as pd

amazon_transactions = pd.read_csv('amazon_transactions.csv')
amazon_transactions['created_at'] = pd.to_datetime(
    amazon_transactions['created_at'])
# find second purchase of each user pandas
amazon_transactions = amazon_transactions.sort_values(['user_id', 'created_at'],
                                                      ascending=[True, True])
# partition by user_id and order by created_at find second purchase created_at
amazon_transactions['prev'] = amazon_transactions.groupby('user_id')[
    'created_at'].shift(1)
amazon_transactions['diff'] = (amazon_transactions['created_at']
                               - amazon_transactions['prev']).dt.days
amazon_transactions = pd.DataFrame(
    amazon_transactions[amazon_transactions['diff'] < 7]['user_id'].unique(),
    columns=['user_id'])
