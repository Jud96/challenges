import pandas as pd

# Start writing code
hotel_reviews[hotel_reviews['hotel_name'] == 'Hotel Arena']\
    .groupby(['hotel_name','reviewer_score']).size().reset_index(name='count')