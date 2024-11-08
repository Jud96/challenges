import pandas as pd

employee = pd.read_csv('number-of-workers-by-department/data.csv')
# department = 'Admin' and joining_date >= '2014-04-01';
len(employee[(employee['joining_date'] >= '2014-04-01') 
             & (employee['department'] == 'Admin')])