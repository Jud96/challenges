import pandas as pd

employee = pd.read_csv('employee-and-manager-salaries/data.csv')
df = employee.merge(employee, left_on='manager_id', right_on='id',
                     suffixes=('_employee', '_manager'))

df[df['salary_employee'] > df['salary_manager']]\
    [['first_name_employee', 'salary_employee']]
