import pandas as pd


df = pd.read_csv("10303-top-percentile-fraud/data.csv")

# find the top 5 percentile fraud score for each state
df2 = df.groupby('state')['fraud_score'].quantile(
    0.95).reset_index(name='top_5_percentile_fraud_score')
# merge the top 5 percentile fraud score with the original dataframe
df2 = df2.merge(df, on='state', how='inner')

df2[df2['fraud_score'] >= df2['top_5_percentile_fraud_score']]
[['policy_num', 'state', 'claim_cost', 'fraud_score']]
