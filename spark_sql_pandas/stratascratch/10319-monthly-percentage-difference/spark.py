# solution
import pyspark.sql.functions as F
from pyspark.sql.functions import col, concat, lit
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "10319-monthly-percentage-difference").getOrCreate()
df = spark.read.csv(
    "10319-monthly-percentage-difference/data.csv", header=True, inferSchema=True)

df = df.withColumn("created_at", df["created_at"].cast("date"))
# format yyyy_mm
df = df.withColumn("created_at", F.date_format("created_at", "yyyy-MM"))

df = df.groupBy('created_at').agg({'value': 'sum'})

# find lead value
windowSpec = Window.orderBy("created_at")
df = df.withColumn("lead_value", F.lead("sum(value)", -1).over(windowSpec))

df = df.withColumn("revenue_diff_pct", F.round(
    (col("sum(value)") - col("lead_value"))*100 / col("lead_value"), 2))

df.select('created_at', 'revenue_diff_pct')
