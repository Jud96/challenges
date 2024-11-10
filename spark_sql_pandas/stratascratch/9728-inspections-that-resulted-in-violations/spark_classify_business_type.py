from pyspark.sql.functions import lower, when, col

df = df.withColumn('business_type', when(lower(col('business_name')).like('%restaurant%'), 'restaurant')
                   .when(lower(col('business_name')).like('%cafe%') | lower(col('business_name')).like('%coffee%') | lower(col('business_name')).like('%café%'), 'cafe')
                   .when(lower(col('business_name')).like('%school%'), 'school')
                   .otherwise('other'))
df = df.select('business_name', 'business_type').distinct()
