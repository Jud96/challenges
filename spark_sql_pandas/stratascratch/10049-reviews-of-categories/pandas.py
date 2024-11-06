import pandas as pd

yelp_business = pd.read_csv('reviews-of-categories/data.csv')

#explode the categories column
# convert the categories column to a list
yelp_business['categories'] = yelp_business['categories'].apply(lambda x: x.split(';'))
# explode the categories column
yelp_business = yelp_business.explode('categories')
yelp_business.groupby('categories')['review_count'].sum()\
    .reset_index().sort_values('review_count', ascending=False)