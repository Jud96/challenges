from pyspark.sql import functions as F
import pyspark

spark = pyspark.sql.SparkSession.builder.appName('find-libraries').getOrCreate()
library_usage = spark.read.csv("find-libraries/data.csv", header=True)
library_usage.filter((F.col('circulation_active_year') == 2016) &
                     (F.col('provided_email_address') == False) &
                     (F.col('notice_preference_definition') == 'email'))\
                     .select('home_library_code').distinct().show()


