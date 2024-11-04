import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("10303-top-percentile-fraud").getOrCreate()
df = spark.read.csv("10303-top-percentile-fraud/data.csv", header=True, inferSchema=True)
df2 = df.groupBy('state').agg(F.expr('percentile(fraud_score, 0.95)')
                              .alias('top_5_percentile_fraud_score'))
df2 = df2.join(df, on='state', how='inner')
df2 = df2.filter(df2['fraud_score'] >= df2['top_5_percentile_fraud_score'])
df2 = df2.select('policy_num','state','claim_cost','fraud_score')