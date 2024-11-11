# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sat_scores = spark.read.csv('data.csv', header=True, inferSchema=True)
# Start writing code
median = sat_scores.agg(F.expr('percentile(sat_writing, 0.5)')\
                        .alias('median')).collect()[0]['median']

sat_scores = sat_scores.filter(sat_scores.sat_writing == median).select('student_id')

# To validate your solution, convert your final pySpark df to a pandas df
sat_scores.toPandas()