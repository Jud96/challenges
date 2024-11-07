import pyspark

from pyspark.sql import functions as F
spark = pyspark.sql.SparkSession.builder.getOrCreate()
df = spark.read.csv("lyft-driver-wages/lyft_drivers.csv", header=True)
df = df.filter((F.col('yearly_salary') < 30000) | (F.col('yearly_salary') >= 70000))