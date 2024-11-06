import pandas as pd

data = pd.read_csv('top-cool-votes/data.csv')
max_cool = data['cool'].max()
data[data['cool'] == max_cool][['business_name', 'review_text']]