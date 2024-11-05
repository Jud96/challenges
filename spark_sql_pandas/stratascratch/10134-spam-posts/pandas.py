import pandas as pd

post = pd.read_csv('10134-spam-posts/facebook_posts.csv')
view = pd.read_csv('10134-spam-posts/facebook_post_views.csv')
df = pd.merge(post, view, on='post_id', how='inner')
df['spam'] = df['post_keywords'].str.contains('spam')
df = df.groupby('post_date').agg(spamcount=('spam', 'sum'),
                                 totalcount=('post_id', 'count')).reset_index()
df['ratio'] = df['spamcount']*100.0 / df['totalcount']
df[['post_date', 'ratio']]
