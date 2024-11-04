# Import your libraries
import pandas as pd

# Start writing code
sf_transactions = pd.read_csv('sf_transactions.csv')
sf_transactions['created_at'] = pd.to_datetime(
    sf_transactions['created_at']).dt.strftime('%Y-%m')
sf_transactions = sf_transactions.groupby(
    'created_at')['value'].sum().reset_index().rename(columns={'value': 'total'})
sf_transactions['prev_month'] = sf_transactions['total'].shift(1)
# Monthly Percentage Difference
sf_transactions['percentage_difference'] = round(
    (sf_transactions['total'] - sf_transactions['prev_month'])
    / sf_transactions['prev_month'] * 100, 2)
sf_transactions = sf_transactions[['created_at', 'percentage_difference']]
