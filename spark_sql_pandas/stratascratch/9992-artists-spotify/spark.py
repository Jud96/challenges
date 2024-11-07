import pyspark

spark = pyspark.sql.SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.csv("artists-spotify/data.csv", header=True)
df.groupBy("artist").count().orderBy("count", ascending=False).show()