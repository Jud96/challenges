from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('number-of-workers-by-department').getOrCreate()

worker = spark.read.csv('number-of-workers-by-department/data.csv', header=True)
worker = worker.filter(worker['joining_date'] >= '2014-04-01')\
    .groupBy('department').count()