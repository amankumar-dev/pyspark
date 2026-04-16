from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())

streamDf=spark.readStream.format('socket').option('host','localhost').option('port',9999).load()

print(streamDf.collect())