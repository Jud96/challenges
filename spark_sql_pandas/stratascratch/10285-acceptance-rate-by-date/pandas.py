# Import your libraries
import pandas as pd


def find_accepted(group):
    # if there is only accepted action then return 1
    if 'accepted' in group.values:
        return 1
    else:
        return 0


def rate(group):
    return group.sum()/group.count()


fb_friend_requests = fb_friend_requests.groupby(['user_id_sender', 'user_id_receiver'])\
    .agg({'date': 'min', 'action': find_accepted}).reset_index()

fb_friend_requests.groupby('date').agg({'action': rate}).reset_index()
