from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("reviews-of-categories").getOrCreate()

yelp_business = spark.read.csv('reviews-of-categories/data.csv',
                               header=True, inferSchema=True)

# convert the categories column to a list
yelp_business = yelp_business.withColumn('categories',
                                         F.split(yelp_business['categories'], ';'))

# explode the categories column
yelp_business = yelp_business.withColumn('categories',
                                         F.explode(yelp_business['categories']))

# aggregate the count of reviews for each category
yelp_business.groupBy('categories').agg({'review_count': 'sum'})\
    .orderBy('sum(review_count)', ascending=False)
