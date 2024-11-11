# Import your libraries
import pyspark
from pyspark.sql.functions import concat_ws, lit,countDistinct
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('flags-per-video').getOrCreate()
user_flags = spark.read.option('header', True).csv('data/user_flags.csv')
user_flags = user_flags.withColumn('full_name',
                                   concat_ws(" ",'user_firstname',
                                          'user_lastname'))
user_flags = user_flags.filter(user_flags['flag_id'].isNotNull())
user_flags = user_flags.groupBy('video_id').agg(countDistinct('full_name')
                                                .alias('num_unique_users'))\
    .select('video_id', 'num_unique_users')
user_flags.toPandas()
