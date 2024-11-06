import pandas as pd

employee = pd.read_csv("income-by-title-and-gende/sf_employee.csv")
bonus = pd.read_csv("income-by-title-and-gende/sf_bonus.csv")

bonus = bonus.groupby('worker_ref_id')['bonus'].sum().reset_index()
df = employee.merge(bonus, left_on='id', right_on='worker_ref_id', how='inner')
df['compensation'] = df['salary'] + df['bonus']
df.groupby(['employee_title','sex'])['compensation'].mean().reset_index()