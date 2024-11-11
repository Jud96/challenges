from pyspark.sql.functions import avg
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv('data.csv', header=True, inferSchema=True)
df.groupBy('city', 'property_type').agg(
    avg('bathrooms').alias('avg_bathrooms'),
    avg('bedrooms').alias('avg_bedrooms')
)