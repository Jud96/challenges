import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('apple-product-counts').getOrCreate()

events = spark.read.csv(
    '10141-apple-product-counts/playbook_events.csv', header=True)
users = spark.read.csv(
    '10141-apple-product-counts/playbook_users.csv', header=True)

df_merged = events.join(users, events.user_id ==
                        users.user_id, how='inner').drop(users.user_id)
df_all_users_agg_lang = df_merged.groupBy('language')\
    .agg(F.countDistinct('user_id').alias('n_total_users'))
df_iphone_users_agg_lang = df_merged\
    .filter(F.col('device')
            .isin('macbook pro', 'iphone 5s', 'ipad air'))\
    .groupBy('language')\
    .agg(F.countDistinct('user_id').alias('n_apple_users'))
df = df_all_users_agg_lang.join(df_iphone_users_agg_lang, 'language', 'left')\
    .fillna(0)\
    .orderBy('n_total_users', ascending=False)
