# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
spark = pyspark.sql.SparkSession.builder.appName("average-salaries").getOrCreate()
employee = spark.read.csv("employee.csv", header=True)
employee = employee.select("department", "first_name", "salary")
df = employee.groupBy("department").agg({"salary": "avg"})
df = df.withColumnRenamed("avg(salary)", "average_salary")
df = df.withColumn("average_salary", F.round(F.col("average_salary"), 2))
employee = employee.join(df, "department", "inner")

# To validate your solution, convert your final pySpark df to a pandas df
employee.toPandas()