import  pandas as pd

fb_asia_energy = pd.read_csv("highest-energy-consumption/fb_asia_energy.csv")
fb_eu_energy = pd.read_csv("highest-energy-consumption/fb_eu_energy.csv")
fb_na_energy = pd.read_csv("highest-energy-consumption/fb_na_energy.csv")

df = pd.concat([fb_asia_energy, fb_eu_energy, fb_na_energy])
df = df.groupby('date')['consumption'].sum().reset_index()
max_consumption = df['consumption'].max()
df = df[df['consumption'] == max_consumption]