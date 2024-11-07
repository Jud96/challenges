import pandas as pd

df = pd.read_csv("lyft_drivers.csv")
df[(df['yearly_salary']< 30000) | (df['yearly_salary']>= 70000)]