# Import your libraries
import pandas as pd

# Start writing code
titanic = pd.read_csv('titanic/data.csv')
df = titanic.pivot_table(index='survived',
                         columns='pclass',
                         values='name',
                         aggfunc='count').reset_index()
df.columns = ['survived','first_class','second_class','third_class']
df