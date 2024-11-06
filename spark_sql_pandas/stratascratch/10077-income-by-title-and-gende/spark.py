from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Income by Title and gender").getOrCreate()

employee = spark.read.csv("income-by-title-and-gende/sf_employee.csv", header=True)
bonus = spark.read.csv("income-by-title-and-gende/sf_bonus.csv", header=True)

bonus = bonus.groupBy("worker_ref_id").agg({"bonus":"sum"})
bonus = bonus.withColumnRenamed("sum(bonus)", "bonus")

df = employee.join(bonus, employee.id == bonus.worker_ref_id, "inner")
df = df.withColumn("compensation", df["salary"] + df["bonus"])
df.groupBy('employee_title','sex').agg({"compensation":"avg"}).show()