

df = pd.read_csv('top-5-states-with-5-star-businesses/yelp_business.csv')

# print top 5 states with 5 star businesses
temp = df[df['stars'] == 5].groupby('state').size().reset_index(name='counts')\
    .sort_values(['counts','state'], ascending=[False,True])

temp['rank'] = temp['counts'].rank(ascending=False)
temp[temp['rank'] <= 5].drop('rank', axis=1)