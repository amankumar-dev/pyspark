from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('temp').getOrCreate()

df1=spark.createDataFrame(
    [('Aman',10),('chaman',20)],
    ['name','age']
)

df2=spark.createDataFrame(
    [('Aman',10),('chaman',20)],
    ['name','age']
)

df1.union(df2).show()