# Import your libraries
import pyspark
import pyspark.sql.functions as F
# Start writing code
spark = pyspark.sql.SparkSession.builder.appName('abc').getOrCreate()
facebook_complaints = spark.read.csv("9781-find-the-rate-of-processed-tickets-for-each-type/data.csv", header=True)
facebook_complaints =facebook_complaints.withColumn("processed",\
                                 F.when(facebook_complaints["processed"]  == True, 1).otherwise(0))
facebook_complaints = facebook_complaints.groupBy("type").agg(F.round(F.sum(F.col("processed"))/F.count(F.col("processed")),2))
# To validate your solution, convert your final pySpark df to a pandas df
facebook_complaints.toPandas()