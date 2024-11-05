import pandas as pd

hosts = pd.read_csv('10156-number-of-units-per-nationality/airbnb_hosts.csv')
units = pd.read_csv('10156-number-of-units-per-nationality/airbnb_units.csv')
df = pd.merge(hosts, units, on='host_id')
df = df[(df.unit_type == 'Apartment') & (df.age < 30)]
df.groupby('nationality').agg({'unit_id':'nunique'}).reset_index()\
    .sort_values('unit_id', ascending=False)