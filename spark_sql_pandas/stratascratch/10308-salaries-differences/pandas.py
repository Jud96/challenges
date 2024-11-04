import pandas as pd

db_employee = pd.read_csv('db_employee.csv')
db_dept = pd.read_csv('db_dept.csv')

df = db_employee.merge(db_dept, left_on='department_id' ,right_on='id', how='inner')
df = df[df['department'].isin(['engineering', 'marketing'])]
df = df.groupby('department').agg({'salary': 'max'})
df.reset_index(inplace=True)
pd.DataFrame([abs(df['salary'][0] - df['salary'][1])], columns=['salary_difference'])