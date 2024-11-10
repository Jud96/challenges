import pandas as pd


def churro_activity_date(df):
    df[(df['facility_name'] == 'STREET CHURROS')\
        & (df['score']  < 95)][['activity_date', 'pe_description']]
    return df