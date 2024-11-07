# Import your libraries
import pyspark
from pyspark.sql import functions as F
# Start writing code
spark = pyspark.sql.SparkSession.builder.getOrCreate()
winemag_p1 = spark.read.csv('wines.csv', header=True, inferSchema=True)
df = winemag_p1.withColumn("description", F.split(F.lower(winemag_p1["description"]),
                                                   '[,;\-\.\/ ]+'))
df = df.filter(F.array_contains(df["description"], "plum") |
           F.array_contains(df["description"], "cherry") | 
           F.array_contains(df["description"], "rose") |
             F.array_contains(df["description"], "hazelnut")).select("winery")
# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()