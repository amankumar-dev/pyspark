from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())

df=spark.createDataFrame(
    [('aman',10),('chaman',20),('dhaman',30)],
    ['name','id']
).show()