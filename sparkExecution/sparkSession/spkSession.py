# The entry point to interact with spark is knows as spark session

from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('test').getOrCreate())
print(type(spark))