import pandas as pd

salesforce_employees = pd.read_csv('highest-target-under-manager/data.csv')

max_target = salesforce_employees[salesforce_employees['manager_id'] == 13]['target'].max()
df = salesforce_employees[(salesforce_employees['target'] == max_target) & 
                          (salesforce_employees['manager_id'] == 13)][['first_name', 'target']]
df.head()