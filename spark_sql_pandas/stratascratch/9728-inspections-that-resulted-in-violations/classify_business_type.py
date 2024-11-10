import pandas as pd

def classify_business_type(df):
    """ 
    Classify the business type based on the business name.
    If the business name contains the word 'restaurant', classify as 'restaurant'.
    If the business name contains the word 'cafe' or 'coffee' or 'café', classify as 'cafe'.
    If the business name contains the word 'school', classify as 'school'.
    Otherwise, classify as 'other'.
    """
    def classify_business(x):
        if 'restaurant' in x.lower():
            return 'restaurant'
        elif 'cafe' in x.lower() or 'coffee' in x.lower() or 'café' in x.lower():
            return 'cafe'
        elif 'school' in x.lower():
            return 'school'
        else:
            return 'other'
    df['business_type'] = df['business_name'].map(lambda x: classify_business(x))
    df[['business_name','business_type']].drop_duplicates()
