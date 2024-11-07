import pandas as pd

employee = pd.read_csv("data.csv")
employee = employee[["department", "first_name", "salary"]]
df = employee.groupby("department")["salary"].mean().reset_index(name="average_salary")
df['average_salary'] = df['average_salary'].round(2)
employee.merge(df, on="department", how="inner")