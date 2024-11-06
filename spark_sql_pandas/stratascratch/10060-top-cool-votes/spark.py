from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('top-cool-votes').getOrCreate()

data = spark.read.csv('top-cool-votes/data.csv', header=True, inferSchema=True)
# select max cool
max_cool = data.agg({"cool": "max"}).collect()[0][0]

data.filter(data['cool'] == max_cool).select('business_name', 'review_text').show()