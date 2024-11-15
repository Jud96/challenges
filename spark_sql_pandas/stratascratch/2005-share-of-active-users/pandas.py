# Import your libraries
import pandas as pd

# Start writing code
fb_active_users = pd.read_csv('fb_active_users.csv')

usa_users = fb_active_users[fb_active_users['country']
                            == 'USA']['user_id'].count()
usa_users_open = fb_active_users[(fb_active_users['country'] == 'USA')
                                 & (fb_active_users['status'] == 'open')]['user_id'].count()
ratio = usa_users_open / usa_users
