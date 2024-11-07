import pandas as pd

customers = pd.read_csv("customers.csv")
orders = pd.read_csv("orders.csv")

merged_data = orders.merge(customers, left_on='cust_id', right_on='id')
merged_data.groupby(['cust_id', 'first_name', 'order_date'])\
    .sum()['total_order_cost'].reset_index().drop('cust_id', axis=1)\
    .sort_values('total_order_cost', ascending=False).head(1)