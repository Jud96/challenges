import pandas as pd

oscar_nominees = pd.read_csv('nominated-for-oscar/data.csv')
# Start writing code
oscar_nominees = oscar_nominees[oscar_nominees['nominee'] == 'Abigail Breslin']   
oscar_nominees['movie'].nunique()