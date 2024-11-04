# add new column date from timestamp
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
from pyspark.sql.functions import to_timestamp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "10352-users-by-avg-session-time").getOrCreate()
df = spark.read.csv("10352-users-by-avg-session-time/facebook_web_log.csv",
                    header=True, inferSchema=True)

df = df.withColumn("date", to_date(col("timestamp")))
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# if page_load_min_timestamp  or page_load_max_timestamp is null thne remove the row
temp = df.groupBy("user_id", "date") \
    .pivot("action") \
    .agg(
        F.max("timestamp").alias("max_timestamp"),
        F.min("timestamp").alias("min_timestamp")
) \
    .filter("page_exit_min_timestamp is not null and page_load_max_timestamp is not null")

temp.groupBy("user_id")\
    .agg(
        F.avg((F.col("page_exit_min_timestamp").cast("long") -
               F.col("page_load_max_timestamp").cast("long")))
    .alias("avg_session_time"))
