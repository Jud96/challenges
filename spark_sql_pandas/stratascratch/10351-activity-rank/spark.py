# add row number to the dataframe
# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "10352-users-by-avg-session-time").getOrCreate()
df = spark.read.csv("10351-activity-rank/data.csv",
                    header=True, inferSchema=True)
df = df.groupBy("from_user").count().orderBy(
    ["count", "from_user"], ascending=[0, 1])
# window = Window.orderBy("count")
# df = df.withColumn("row_num", row_number().over(window))
df = df.withColumn("row_num", monotonically_increasing_id() + 1)
