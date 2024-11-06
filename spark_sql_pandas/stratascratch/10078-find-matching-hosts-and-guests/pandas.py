import pandas as pd

guests = pd.read_csv('find-matching-hosts-and-guests/airbnb_guests.csv')
hosts = pd.read_csv('find-matching-hosts-and-guests/airbnb_hosts.csv')

df = guests.merge(hosts, on=['nationality', 'gender'], how='inner')[['guest_id', 'host_id']]
df.drop_duplicates(inplace=True)