import pyspark.sql.session as session

spark = session.SparkSession.builder.appName("app").getOrCreate()

# Load the data
orders = spark.read.csv("9782-customer-revenue-in-march/data.csv", header=True)
# process the data
orders.filter(orders.order_date.between('2019-03-01', '2019-03-31'))\
    .groupBy('cust_id').agg({'total_order_cost': 'sum'})\
    .withColumnRenamed('sum(total_order_cost)', 'total_order_cost')\
    .orderBy('total_order_cost', ascending=False).show()