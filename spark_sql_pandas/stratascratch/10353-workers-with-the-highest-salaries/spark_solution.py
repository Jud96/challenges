import pyspark
from pyspark.sql import SparkSession

# create the Spark session
spark = SparkSession.builder.appName("10353-workers-with-the-highest-salaries").getOrCreate()
# read the data
worker = spark.read.csv("worker.csv", header=True, inferSchema=True)
title = spark.read.csv("title.csv", header=True, inferSchema=True)
# find the highest salary
max_salary = worker.agg({"salary": "max"}).collect()[0][0]
# find the workers with the highest salary
worker_with_max_salary = worker.filter(worker.salary == max_salary)
# join the worker with the title 
# select the title of the worker with the highest salary
# rename the column to best_paid_title
worker_with_max_salary\
    .join(title, worker_with_max_salary.worker_id == title.worker_ref_id)\
    .select('worker_title')\
    .withColumnRenamed('worker_title', 'best_paid_title')\
    .show()