# Import your libraries
import pandas as pd

# Start writing code
online_orders = pd.read_csv('online_orders.csv')
online_orders['revenue'] = online_orders['units_sold'] * online_orders['cost_in_dollars']
online_orders['date'] = pd.to_datetime(online_orders['date'])
online_orders[online_orders['date'].between('2022-01-01', '2022-06-30')]\
    .groupby('product_id').agg({'revenue':'sum'}).reset_index()\
        .sort_values('revenue', ascending=False).head(5)