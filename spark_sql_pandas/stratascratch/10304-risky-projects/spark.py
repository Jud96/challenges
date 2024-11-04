from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("10304-risky-projects").getOrCreate()

linkedin_projects = spark.read.csv("10304-risky-projects/linkedin_projects.csv",
                                   header=True, inferSchema=True)
linkedin_emp_projects = spark.read.csv("10304-risky-projects/linkedin_emp_projects.csv",
                                       header=True, inferSchema=True)
linkedin_employees = spark.read.csv("10304-risky-projects/linkedin_employees.csv",
                                    header=True, inferSchema=True)

df = linkedin_projects.join(linkedin_emp_projects,
                            linkedin_projects.id == linkedin_emp_projects.project_id, 'inner')\
    .join(linkedin_employees, linkedin_emp_projects.emp_id == linkedin_employees.id, 'inner')
df = df.withColumn('start_date', F.col('start_date').cast('date'))
df = df.withColumn('end_date', F.col('end_date').cast('date'))
df = df.withColumn('duration', F.datediff('end_date', 'start_date'))
df = df.withColumn('prorated_expense', df.duration * df.salary/365.0)
df = df.groupBy(['title', 'budget']).agg(F.ceil(F.sum(F.col('prorated_expense')))
                                         .alias('prorated_expense')).orderBy('title')
df = df.filter(df.budget < df.prorated_expense)
