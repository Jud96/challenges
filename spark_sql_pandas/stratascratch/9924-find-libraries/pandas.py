import pandas as pd

library_usage = pd.read_csv("data.csv")

library_usage[(library_usage['circulation_active_year'] == 2016) &
              (library_usage['provided_email_address'] == False) &
              (library_usage['notice_preference_definition'] == 'email')]
[['home_library_code']].drop_duplicates(subset='home_library_code')
