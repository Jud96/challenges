import pyspark

# Create a Spark session
spark = pyspark.sql.SparkSession.builder.getOrCreate()

# Load data
customers = spark.read.csv('customers.csv', header=True)
orders = spark.read.csv('orders.csv', header=True)

# Start writing code
merged_data = customers.join(orders, customers.id == orders.cust_id, how='inner')
merged_data = merged_data.groupBy(['cust_id', 'first_name', 'order_date'])\
    .agg({'total_order_cost': 'sum'})
merged_data = merged_data.orderBy('sum(total_order_cost)', ascending=False)\
    .drop('cust_id').limit(1)