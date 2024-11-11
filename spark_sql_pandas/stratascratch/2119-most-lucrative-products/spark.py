import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('most-lucrative-products').getOrCreate()
online_orders = spark.read.option('header', True).csv('data/online_orders.csv')
online_orders = online_orders.withColumn('revenue',
                                         online_orders['units_sold'] * online_orders['cost_in_dollars'])
online_orders = online_orders.withColumn(
    'date', F.to_date(online_orders['date']))
online_orders = online_orders.filter(online_orders['date']
                                     .between('2022-01-01', '2022-06-30'))\
    .groupBy('product_id').agg(F.sum('revenue').alias('revenue'))\
    .orderBy(F.desc('revenue')).limit(5)
