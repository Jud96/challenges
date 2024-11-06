from  pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example").getOrCreate()

fb_asia_energy = spark.read.csv("highest-energy-consumption/fb_asia_energy.csv",
                                 header=True, inferSchema=True)
fb_eu_energy = spark.read.csv("highest-energy-consumption/fb_eu_energy.csv", 
                              header=True, inferSchema=True)
fb_na_energy = spark.read.csv("highest-energy-consumption/fb_na_energy.csv",
                               header=True, inferSchema=True)

df = fb_asia_energy.union(fb_eu_energy).union(fb_na_energy)
df = df.groupBy("date").agg({"consumption": "sum"})
df = df.withColumnRenamed("sum(consumption)", "consumption")
max_consumption = df.agg({"consumption": "max"}).collect()[0][0]
df.filter(df.consumption == max_consumption).show()