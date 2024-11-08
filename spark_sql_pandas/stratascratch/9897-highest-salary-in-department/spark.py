from pyspark.sql import functions as F
import pyspark

spark = pyspark.sql.SparkSession.builder.appName('highest-salary-in-department')\
    .getOrCreate()
employee = spark.read.csv('highest-salary-in-department/data.csv', header=True)
emp_with_max_salary = employee.groupBy('department').agg(F.max('salary').alias('salary'))\
    .join(employee, on=['department', 'salary'], how='inner')\
    .select('department', 'first_name', 'salary')