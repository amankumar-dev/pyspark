from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('temp').getOrCreate()

df=spark.read.json('path',multiLine=True,inferSchema=True)

