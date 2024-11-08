import pandas as pd

employee = pd.read_csv('highest-salary-in-department/data.csv')
emp_with_max_salary = employee.groupby(
    'department')['salary'].max().reset_index()
emp_with_max_salary = emp_with_max_salary.merge(employee,
                                                on=['department', 'salary'], how='inner')\
                                                    [['department', 'first_name', 'salary']]

emp_with_max_salary.head()
