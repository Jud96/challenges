import pandas as pd

facebook_hack_survey = pd.read_csv("popularity-of-hack/facebook_hack_survey.csv")
facebook_employees =  pd.read_csv("popularity-of-hack/facebook_employees.csv")

df = facebook_employees.merge(facebook_hack_survey, left_on='id',
                               right_on='employee_id', how='inner')
df.groupby('location')['popularity'].mean().reset_index()