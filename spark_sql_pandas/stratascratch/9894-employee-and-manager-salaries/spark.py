from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('employee-and-manager-salaries').getOrCreate()

employee = spark.read.csv('employee-and-manager-salaries/data.csv', header=True)
manager= employee.select('id', 'salary').withColumnRenamed('salary', 'salary_manager')
df = employee.join(manager, employee.manager_id == manager.id, 'inner')
df = df.filter(df['salary'] > df['salary_manager'])\
    .select('first_name', 'salary', 'salary_manager')
    

df.show()    