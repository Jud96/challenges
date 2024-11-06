import pandas as pd

posts = pd.read_csv('all-posts-which-were-reacted-to-with-a-heart/facebook_posts.csv')
reactions = pd.read_csv('all-posts-which-were-reacted-to-with-a-heart/\
                        facebook_reactions.csv')

df = pd.merge(posts, reactions, on='post_id', how='inner')
df =df[df['reaction'] == 'heart']
df = df.drop_duplicates(subset='post_id', keep='first')
df[['post_id', 'poster_x','post_text', 'post_keywords','post_date']]