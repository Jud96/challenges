from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('abc').getOrCreate()

df = spark.read.csv('10159-ranking-most-active-guests/data.csv',
                     header=True, inferSchema=True)
df = df.groupBy('id_guest').agg({'n_messages': 'sum'})
df = df.withColumnRenamed('sum(n_messages)', 'n_messages')


windowSpec = Window.orderBy(col('n_messages').desc())
df = df.withColumn('dense_rank', dense_rank().over(windowSpec))