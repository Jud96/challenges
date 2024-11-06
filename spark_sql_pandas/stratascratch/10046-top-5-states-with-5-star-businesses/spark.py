from  pyspark.sql import SparkSession

spark = SparkSession.builder.appName("top-5-states-with-5-star-businesses").getOrCreate()

df = spark.read.csv('top-5-states-with-5-star-businesses/yelp_business.csv',
                     header=True, inferSchema=True)

temp = df.filter(df['stars'] == 5).groupBy('state').count().sort('count', ascending=False)
min_value = temp.collect()[4][1] # get the 5th value
temp = temp.filter(temp['count'] >= min_value)