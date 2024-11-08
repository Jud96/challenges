# Import your libraries
import pyspark
from pyspark.sql import functions as F
# Start writing code
spark = pyspark.sql.SparkSession.builder.appName("example").getOrCreate()
worker = spark.read.csv('data.csv', header=True, inferSchema=True)
worker = worker.withColumn(
    "joining_date", F.to_date(worker["joining_date"])
)
worker = worker.withColumn("month", F.month(worker["joining_date"]))
april_df = worker.filter(worker["month"] >= 4)
result = april_df.filter(april_df["department"] == "Admin").count()

worker = worker.filter((worker['joining_date'] >= '2014-04-01')
                        & (worker['department'] == 'Admin')).count()