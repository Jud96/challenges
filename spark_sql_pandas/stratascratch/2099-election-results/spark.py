from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('election_results').getOrCreate()
voting_results = spark.read.table('voting_results')

voting_results = voting_results.withColumn('vote_value',
                        f.lit(1.0)/f.count('voter').over(Window.partitionBy('voter')))
voting_results = voting_results.filter(voting_results['candidate'].isNotNull())

df = voting_results.groupBy('candidate').agg({'vote_value': 'sum'})\
    .withColumnRenamed('sum(vote_value)', 'vote_value')

df = df.withColumn('rank', f.rank().over(Window.orderBy(f.desc('vote_value'))))

df.filter(df['rank'] == 1).select('candidate')