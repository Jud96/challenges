import pyspark

spark = pyspark.sql.SparkSession.builder.appName('titanic').getOrCreate()

titanic = spark.read.csv('titanic/data.csv', header=True)
titanic = titanic.groupBy('survived').pivot('pclass').count()
titanic = titanic.withColumnRenamed('1','first_class')\
    .withColumnRenamed('2','second_class')\
        .withColumnRenamed('3','third_class')
