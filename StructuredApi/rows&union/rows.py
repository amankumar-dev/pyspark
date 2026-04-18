from pyspark.sql import Row,SparkSession

r=Row(name='Aman',dept='IT',salary=30000)
spark=SparkSession.builder.appName('temp').getOrCreate()

df=spark.createDataFrame(
    [r]
).show()