# Import your libraries
import pyspark

# Start writing code
hotel_reviews = hotel_reviews.filter(hotel_reviews.hotel_name == 'Hotel Arena')\
    .groupBy('hotel_name','reviewer_score').count()

# To validate your solution, convert your final pySpark df to a pandas df
hotel_reviews.toPandas()