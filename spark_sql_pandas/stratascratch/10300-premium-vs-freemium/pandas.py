import pandas as pd

user = pd.read_csv('10300-premium-vs-freemium/user.csv')
account = pd.read_csv('10300-premium-vs-freemium/account.csv')
fact = pd.read_csv('10300-premium-vs-freemium/fact.csv')

df = pd.merge(user, account, on='acc_id').merge(fact, on='user_id')
df = df.pivot_table(index='date',
                    columns='paying_customer',
                    values='downloads',
                    aggfunc='sum')
df = df.reset_index()
df.columns = ['date', 'no', 'yes']
df = df[df['no'] > df['yes']]
