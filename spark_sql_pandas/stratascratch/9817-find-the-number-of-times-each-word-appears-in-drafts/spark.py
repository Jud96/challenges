# Import your libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Start a simple Spark Session
spark = SparkSession.builder.appName("example").getOrCreate()
google_file_store = spark.read.csv("data.csv", header=True)
# Start writing code
drafts = google_file_store.filter(google_file_store.filename.contains('draft'))\
    .select('contents')
df = drafts.withColumn('contents', 
                       F.explode(F.split(F.lower(F.col('contents')), '\W+')))\
                        .groupBy('contents').count().filter(F.col('contents') != '')

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()