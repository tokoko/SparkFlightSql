from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('abc').getOrCreate()

print(spark.version)


