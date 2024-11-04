import pandas as pd

google_gmail_emails = pd.read_csv("10351-activity-rank/data.csv")
google_gmail_emails = google_gmail_emails.groupby('from_user').size()\
    .reset_index(name='total_emails')
google_gmail_emails = google_gmail_emails.sort_values(by=['total_emails', 'total_emails'],
                                                      ascending=[False, True])
google_gmail_emails['rank'] = range(1, 1+len(google_gmail_emails))
