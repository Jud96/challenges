from pyspark.sql import functions as F
import pyspark

spark = pyspark.sql.SparkSession.builder.appName('highest-target-under-manager')\
    .getOrCreate()
salesforce_employees = spark.read.csv('highest-target-under-manager/data.csv',
                                       header=True)
max_target = salesforce_employees.filter(F.col('manager_id') == 13)\
.agg(F.max('target').alias('max_target')).collect()[0][0]
df = salesforce_employees.filter((F.col('target') == max_target)
                                  & (F.col('manager_id') == 13))\
                                    .select('first_name', 'target')
df.show()