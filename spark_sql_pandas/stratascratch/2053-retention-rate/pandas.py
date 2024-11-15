import pandas as pd

sf_events = pd.read_csv('sf_events.csv')

dec_2020 = sf_events[

    (sf_events["date"].dt.year == 2020)

    & (sf_events["date"].dt.month == 12)

][["account_id", "user_id"]].drop_duplicates()

jan_2021 = sf_events[

    (sf_events["date"].dt.year == 2021)

    & (sf_events["date"].dt.month == 1)

][["account_id", "user_id"]].drop_duplicates()



max_date = (

    sf_events.groupby("user_id")["date"]

    .max()

    .to_frame("max_date")

    .reset_index()

)



dec_2020 = dec_2020.merge(max_date, on="user_id")

jan_2021 = jan_2021.merge(max_date, on="user_id")



dec_2020["retention"] = 0

jan_2021["retention"] = 0



dec_2020.loc[dec_2020["max_date"] > "2020-12-31", "retention"] = 1

jan_2021.loc[jan_2021["max_date"] > "2021-01-31", "retention"] = 1



retention_dec = (

    dec_2020.groupby("account_id")["retention"]

    .mean()

    .to_frame("dec_retention")

    .reset_index()

)

retention_jan = (

    jan_2021.groupby("account_id")["retention"]

    .mean()

    .to_frame("jan_retention")

    .reset_index()

)



merged = retention_dec.merge(retention_jan, on="account_id")

merged["retention"] = (

    merged["jan_retention"] / merged["dec_retention"]

)



result = merged[["account_id", "retention"]]