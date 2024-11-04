# Import your libraries
import pandas as pd


def most_profitable_companies(forbes_global_2010_2014):
    """ Returns the top 3 most profitable companies from 2010 to 2014 """
    return forbes_global_2010_2014.sort_values(by='profits', ascending=False)\
        .head(3)[['company', 'profits']]
