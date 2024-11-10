import pyspark.sql.functions as F
import pyspark 

spark = pyspark.sql.SparkSession.builder.appName("app").getOrCreate()

sf_restaurant_health_violations = spark.read.csv("data.csv", 
                                                 header=True, inferSchema=True)

filtered_df = sf_restaurant_health_violations.\
    filter("(business_name == 'Roxanne Cafe') and (violation_id is not null)")
filtered_df.groupBy(F.year("inspection_date").alias("year"))\
    .agg(F.count("violation_id").alias("count")).show()