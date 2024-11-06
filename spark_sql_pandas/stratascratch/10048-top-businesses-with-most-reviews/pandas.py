import pandas as pd

df = pd.read_csv('top-businesses-with-most-reviews/data.csv')
df.sort_values(by='review_count', ascending=False, inplace=True)
df[['name','review_count']].head(5)