import numpy as np
import pandas as pd

linkedin_projects = pd.read_csv("10304-risky-projects/linkedin_projects.csv")
linkedin_emp_projects = pd.read_csv("10304-risky-projects/linkedin_emp_projects.csv")
linkedin_employees = pd.read_csv("10304-risky-projects/linkedin_employees.csv")

df = linkedin_projects.merge(linkedin_emp_projects, left_on='id', right_on='project_id')\
    .merge(linkedin_employees, left_on='emp_id', right_on='id')
df['start_date'] = pd.to_datetime(df['start_date'])
df['end_date'] = pd.to_datetime(df['end_date'])

df['duration'] = (df['end_date'] -df['start_date']).dt.days
df['prorated_expense'] =  (df['duration'] * df['salary'])/365.0

df = df.groupby(['title','budget'])['prorated_expense'].sum()\
    .reset_index().sort_values('title')
# ceil the prorated_expense
df['prorated_expense'] = np.ceil(df['prorated_expense'])
df[df['prorated_expense'] > df['budget']]