from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Top Businesses with Most Reviews").getOrCreate()

df = spark.read.csv('top-businesses-with-most-reviews/data.csv',
                     header=True, inferSchema=True)

df.select('name','review_count').orderBy(df['review_count'].desc()).limit(5).show()