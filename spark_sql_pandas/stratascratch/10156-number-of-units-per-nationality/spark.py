from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('abc').getOrCreate()

hosts = spark.read.csv(
    '10156-number-of-units-per-nationality/airbnb_hosts.csv', header=True)
units = spark.read.csv(
    '10156-number-of-units-per-nationality/airbnb_units.csv', header=True)


df = hosts.join(units, hosts.host_id == units.host_id, 'inner')
df = df.filter((df.age < 30) & (df.unit_type == 'Apartment'))
df.groupBy('nationality').agg(F.countDistinct(F.col('unit_id')).alias('count'))
