from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("10308-salaries-differences").getOrCreate()
db_employee = spark.read.csv("db_employee.csv", header=True, inferSchema=True)
db_dept = spark.read.csv("db_dept.csv", header=True, inferSchema=True)
df = db_employee.join(db_dept, db_employee.department_id == db_dept.id, 'inner')
df = df.filter(col('department').isin(['engineering', 'marketing']))
df = df.groupBy().pivot('department').agg({'salary': 'max'})
# find difference between max salary of engineering and marketing in col 'max(salary)'
df.withColumn('salary_difference',F.abs(col('engineering')-col('marketing')))\
    .select('salary_difference')

