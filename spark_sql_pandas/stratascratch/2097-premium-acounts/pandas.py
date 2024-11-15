import pandas as pd
import numpy as np

premium_accounts_by_day = pd.read_csv("premium_accounts.csv")
premium_accounts_by_day["date_plus_7d"] = premium_accounts_by_day["entry_date"]
+ pd.DateOffset(7)
merged_df = pd.merge(premium_accounts_by_day, premium_accounts_by_day, how="left",
                     left_on=["account_id", "date_plus_7d"],
                     right_on=["account_id", "entry_date"],
                     suffixes=("_left", "_right"))
merged_df = merged_df[merged_df["final_price_left"] > 0]
merged_df["final_price_right"] = merged_df["final_price_right"].replace({
                                                                        0: np.nan})
result = merged_df.groupby("entry_date_left")\
    .count()[["final_price_left","final_price_right"]].reset_index()\
        .rename(columns={"entry_date_left": "date",
                "final_price_left": "premium_paid_accounts",
                "final_price_right": "premium_paid_accounts_after_7d"
                }).sort_values("date").head(7)
