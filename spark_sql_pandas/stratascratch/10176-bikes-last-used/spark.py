# Import your libraries
import pyspark

# Start writing code
dc_bikeshare_q1_2012 = dc_bikeshare_q1_2012.groupby('bike_number')\
    .agg({'end_time':'max'}).orderBy('max(end_time)', ascending=False)

# To validate your solution, convert your final pySpark df to a pandas df
dc_bikeshare_q1_2012.toPandas()