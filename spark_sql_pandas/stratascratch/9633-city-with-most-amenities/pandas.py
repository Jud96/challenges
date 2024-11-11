# Import your libraries
import pandas as pd

def city_with_most_amenities(airbnb_search_details):
    # read in the data
    df = airbnb_search_details[['amenities','city']]
    df['amenities'] = df['amenities'].apply(lambda x : len(x.split(",")))
    df = df.groupby('city').sum().reset_index()\
        .sort_values('amenities',ascending=False)[['city']]

    return df.iloc[0]['city']
