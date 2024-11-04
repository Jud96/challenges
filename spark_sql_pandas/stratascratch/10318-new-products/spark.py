import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("10318-new-products").getOrCreate()
car_launches = spark.read.csv("10318-new-products/data.csv", header=True, inferSchema=True)
car_launches = car_launches.groupBy('company_name').pivot('year').count()
car_launches = car_launches.withColumn('net_new_products',\
                                        car_launches['2020'] - car_launches['2019'])
car_launches = car_launches.select('company_name','net_new_products')