from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('find-matching-hosts-and-guests').getOrCreate()

# Load the data
guests = spark.read.csv('find-matching-hosts-and-guests/airbnb_guests.csv', header=True)
hosts = spark.read.csv('find-matching-hosts-and-guests/airbnb_hosts.csv', header=True)

df = guests.join(hosts, (guests['nationality'] == hosts['nationality'] ) 
            & (guests['gender'] == hosts['gender']), 'inner')\
                .select(guests['guest_id'], hosts['host_id'])\
                .drop_duplicates()
                
df.toPandas()