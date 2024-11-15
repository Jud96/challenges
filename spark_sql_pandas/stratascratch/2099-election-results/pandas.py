# Import your libraries
import pandas as pd

voting_results = pd.read_csv('election_results.csv')
voting_results['vote_value'] = voting_results.groupby('voter')\
    .transform(lambda x: 1.0/len(x))
df = voting_results.groupby('candidate').agg({'vote_value': 'sum'}).reset_index()
df['rank'] = df['vote_value'].rank(ascending=False)
df[df['rank'] == 1][['candidate']]