from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())

df=spark.read.csv('path',header=True,inferSchema=True)
df.show()