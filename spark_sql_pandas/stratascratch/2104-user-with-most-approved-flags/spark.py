from pyspark.sql.functions import countDistinct, concat, lit
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('user-with-most-approved-flags').getOrCreate()
user_flags = spark.read.option('header', True).csv('data/user_flags.csv')

df = user_flags.filter(user_flags.flag_id.isNotNull())\
    .groupBy(user_flags.user_firstname, user_flags.user_lastname)\
    .agg(countDistinct(user_flags.video_id).alias('approved_flags'))

max_flags = df.agg({'approved_flags':'max'}).collect()[0][0]

df = df.filter(df.approved_flags == max_flags)
df = df.withColumn('full_name', concat(df.user_firstname, lit(' '), df.user_lastname))
df.select('full_name').toPandas()