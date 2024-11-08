import pandas as pd

employee = pd.read_csv('number-of-workers-by-department/data.csv')
employee[employee['joining_date'] >= '2014-04-01']\
    .groupby('department').size().reset_index(name='count')