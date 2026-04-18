from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('temp').getOrCreate()

df=spark.createDataFrame(
    [('Aman',10),('chaman',20)],
    ['name','age']
)

df=df.drop('age')
df.show()