from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F


def is_accepted_exists(x):
    # if any accepted then return 1 else 0
    return 1 if 'accepted' in x else 0


is_accepted_udf = F.udf(is_accepted_exists, IntegerType())

df = df.groupBy('user_id_sender', 'user_id_receiver')\
    .agg(
        F.min('date').alias('first_date'),
        F.max(is_accepted_udf('action')).alias('accepted_exists')
)
df = df.orderBy('first_date')
df.groupBy('first_date').agg(F.sum('accepted_exists') /
                             F.count('accepted_exists')).show()
