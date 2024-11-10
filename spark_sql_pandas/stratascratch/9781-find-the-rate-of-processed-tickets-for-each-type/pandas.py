# Import your libraries
import pandas as pd

# Start writing code
facebook_complaints = pd.read_csv('data.csv')
 
df = facebook_complaints.groupby('type').agg(
    count = ('processed', 'count'),
    count_processed = ('processed', 'sum'),
).reset_index()
df['rate'] = df['count_processed'] *1.0 / df['count']
df.drop(columns=['count', 'count_processed'], inplace=True)
df