from pyspark.sql import SparkSession


spark = SparkSession \
.builder \
.appName("Python Spark SQL basic example") \
.getOrCreate()

facebook_hack_survey = spark.read.csv("popularity-of-hack/facebook_hack_survey.csv",
                                       header=True, inferSchema=True)
facebook_employees = spark.read.csv("popularity-of-hack/facebook_employees.csv",
                                     header=True, inferSchema=True)

df = facebook_employees.join(facebook_hack_survey, 
                             facebook_employees.id ==  facebook_hack_survey.employee_id,
                             'inner')
df.groupBy('location').agg({'popularity':'avg'}).show()

