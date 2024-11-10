# Import your libraries
import pandas as pd

sf_restaurant_health_violations = pd.read_csv('data.csv')
df = sf_restaurant_health_violations[(sf_restaurant_health_violations['business_name']
                                 == 'Roxanne Cafe')
                                & ((sf_restaurant_health_violations['violation_id']).notnull())]
df['inspection_date_year'] = pd.to_datetime(df['inspection_date']).dt.year

df.groupby('inspection_date_year').agg({'inspection_id':'count'}).reset_index()