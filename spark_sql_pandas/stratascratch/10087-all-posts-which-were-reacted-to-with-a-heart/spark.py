from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("all-posts-which-were-reacted-to-with-a-heart")\
    .getOrCreate()

posts = spark.read.csv('all-posts-which-were-reacted-to-with-a-heart/facebook_posts.csv', 
                       header=True)
reactions = spark.read.csv('all-posts-which-were-reacted-to-with-a-heart/\
                           facebook_reactions.csv', 
                           header=True)


df = posts.join(reactions, posts.post_id == reactions.post_id, 'inner')\
    .drop(reactions.post_id).drop(reactions.poster)
df = df.filter(df.reaction == 'heart')
df = df.dropDuplicates(['post_id'])
df = df.select('post_id', 'poster','post_text', 'post_keywords','post_date')
df.toPandas()