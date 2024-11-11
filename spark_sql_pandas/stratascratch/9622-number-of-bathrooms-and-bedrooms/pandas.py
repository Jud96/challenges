# Import your libraries
import pandas as pd
import numpy as np
# Start writing code
def avg_bathrooms_bedrooms(airbnb_search_details):
    return airbnb_search_details.groupby(['city', 'property_type']).agg(
            avg_bathrooms = ('bathrooms', np.mean),
            avg_bedrooms = ('bedrooms', np.mean),
        ).reset_index()