import pandas as pd

orders = pd.read_csv('data.csv')

orders[orders['order_date'].between('2019-03-01', '2019-03-31')]\
.groupby('cust_id')['total_order_cost'].sum().reset_index()\
.sort_values('total_order_cost', ascending=False)