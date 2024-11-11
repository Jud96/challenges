# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Start your Spark session
spark = SparkSession.builder.appName("app").getOrCreate()
airbnb_search_details = spark.read.csv('data.csv', header=True, inferSchema=True)

airbnb_search_details = airbnb_search_details\
    .withColumn("amenities_len",F.size(F.split(F.col("amenities"),",")))
airbnb_search_details = airbnb_search_details.groupBy("city")\
    .agg(F.sum("amenities_len").alias("num_amenities"))\
    .orderBy(F.desc("num_amenities")).select("city").limit(1)
# To validate your solution, convert your final pySpark df to a pandas df
airbnb_search_details.toPandas()