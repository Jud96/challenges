import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('nominated-for-oscar').getOrCreate()

oscar_nominees = spark.read.csv('data.csv', header=True)

oscar_nominees = oscar_nominees.filter(oscar_nominees['nominee'] == 'Abigail Breslin')
oscar_nominees = oscar_nominees.agg(F.countDistinct("movie"))
# To validate your solution, convert your final pySpark df to a pandas df
oscar_nominees.toPandas()