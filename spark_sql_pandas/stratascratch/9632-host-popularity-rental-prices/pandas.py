#  host id concat between price, room_type, host_since, zipcode, number_of_reviews
import numpy as np
import pandas as pd

df = pd.read_csv('data.csv')
df["host_id"] = (
    df["price"].map(str)
    + df["room_type"].map(str)
    + df["host_since"].map(str)
    + df["zipcode"].map(str)
    + df["number_of_reviews"].map(str)
)

df = df[["host_id", "number_of_reviews", "price"]].drop_duplicates()

df['host_pop_rating'] = pd.cut(df['number_of_reviews'],
                               bins=[-np.inf, 0, 5, 15, 40, np.inf],
                               labels=['New', 'Rising', 'Trending Up', 'Popular', 'Hot'])

df = df.groupby('host_pop_rating').agg(
    min_price=('price', 'min'),
    avg_price=('price', np.mean),
    max_price=('price', 'max'),
).reset_index()
# check if host_pop_rating and host_popularity are the same
