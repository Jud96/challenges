from pyspark.sql.functions import col, datediff
from pyspark.sql.window import Window
from pyspark.sql.functions import lead
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("0322-finding-user-purchases").getOrCreate()
df = spark.read.csv('amazon_transactions.csv', header=True, inferSchema=True)

df = df.withColumn("created_at", df["created_at"].cast("timestamp"))
windowSpec = Window.partitionBy("user_id").orderBy("created_at")
df = df.withColumn("next_purchase_date", lead("created_at", 1).over(windowSpec))
df = df.withColumn("diff", datediff(col("next_purchase_date"),col("created_at")))
df = df.filter(col("diff") < 7).select("user_id").distinct()