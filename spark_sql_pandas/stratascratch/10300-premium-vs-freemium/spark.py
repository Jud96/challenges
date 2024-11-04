from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("10300-premium-vs-freemium").getOrCreate()
user = spark.read.csv('10300-premium-vs-freemium/user.csv', header=True)
account = spark.read.csv('10300-premium-vs-freemium/account.csv', header=True)
fact = spark.read.csv('10300-premium-vs-freemium/fact.csv', header=True)

df = user.join(account,user.acc_id == account.acc_id).join(fact,user.user_id == fact.user_id)
df = df.orderBy('date')
df =df.groupBy('date').pivot('paying_customer').agg({'downloads':'sum'})
df.filter(df.no >df.yes)