import pandas as pd

orders = pd.read_csv('orders.csv')
customers = pd.read_csv('customers.csv')
df = orders.merge(customers, left_on='cust_id' ,right_on='id' , how='inner')
df[~df['address'].isnull() ].shape[0]*100/df.shape[0]
