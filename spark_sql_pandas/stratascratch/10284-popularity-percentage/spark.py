from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('popularity_percentage').getOrCreate()

df = spark.read.csv('facebook_friends.csv', header=True, inferSchema=True)

num_of_users = df.select('user1').union(df.select('user2'))\
    .agg(F.countDistinct('user1').alias('count')).collect()[0]['count']
df2 = df.select('user2', 'user1').withColumnRenamed('user1', 'user2')
df = df.union(df2).dropDuplicates()
df = df.groupBy('user1').agg(F.count('user2')*100/num_of_users)
df.orderBy('user1').show()
