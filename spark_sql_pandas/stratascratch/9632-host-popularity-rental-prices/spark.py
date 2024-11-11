import pyspark.sql.functions as F
from pyspark.sql.functions import col, when
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('host_popularity_rental_prices').getOrCreate()

df = spark.read.csv('data.csv', header=True, inferSchema=True)
# concat fields to create host_id 
df = df.withColumn('host_id', 
                   F.concat(F.col('price').cast('string'), 
                          F.col('room_type').cast('string'), 
                          F.col('host_since').cast('string'), 
                          F.col('zipcode').cast('string'), 
                          F.col('number_of_reviews').cast('string')))
# drop duplicates
df = df.select('host_id', 'number_of_reviews', 'price').dropDuplicates()
# cut number_of_reviews into bins
df = df.withColumn('host_pop_rating', 
                    when(F.col('number_of_reviews') <= 0, 'New')
                   .when(F.col('number_of_reviews') <= 5, 'Rising')
                   .when(F.col('number_of_reviews') <= 15, 'Trending Up')
                   .when(F.col('number_of_reviews') <= 40, 'Popular')
                   .otherwise('Hot'))

df = df.groupBy('host_pop_rating').agg(F.min('price').alias('min_price'),
                                        F.avg('price').alias('avg_price'),
                                        F.max('price').alias('max_price'))