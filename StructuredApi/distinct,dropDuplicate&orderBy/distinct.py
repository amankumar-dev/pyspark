from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('temp').getOrCreate()
df=spark.createDataFrame(
    [('aman',10),('aman',10)],
    ['name','age']
)

df.distinct().show()