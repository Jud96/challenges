from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('percentage_of_shipable_orders').getOrCreate()


orders = spark.read.csv('percentage_of_shipable_orders/orders.csv', header=True)
customers = spark.read.csv('percentage_of_shipable_orders/customers.csv', header=True)

df = orders.join(customers, orders.cust_id == customers.id, 'inner') 
df.filter(df.address.isNotNull()).count()*100/df.count()