import pandas as pd

df = pd.read_csv('data.csv')
df = df.pivot_table(index=['company_name'],
                    columns=['year'],
                    values='product_name',
                    aggfunc='size').reset_index()
df.rename(columns={2019: '2019', 2020: '2020'}, inplace=True)
df['net_new_products'] = df['2020'] - df['2019']
df[['company_name', 'net_new_products']]
