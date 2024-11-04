import pandas as pd

worker = pd.read_csv('worker.csv')
title = pd.read_csv('title.csv')

worker[worker['salary'] == worker['salary'].max()]\
    .merge(title, left_on='worker_id', right_on='worker_ref_id')[['worker_title']]\
    .rename(columns={'worker_title': 'best_paid_title'})
