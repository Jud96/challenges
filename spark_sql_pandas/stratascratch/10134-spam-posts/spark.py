import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('spam-posts').getOrCreate()

post = spark.read.csv('10134-spam-posts/facebook_posts.csv', header=True)
view = spark.read.csv('10134-spam-posts/facebook_post_views.csv', header=True)
df = post.join(view, post.post_id == view.post_id, 'inner').drop(view.post_id)
df = df.withColumn('spam', df.post_keywords.contains('spam'))
df = df.withColumn('spam', F.when(df.spam == True, 1).otherwise(0))
df.groupBy('post_date').agg((F.sum(F.col('spam'))*100.0/F.count(F.col('post_id'))).alias('ratio'))