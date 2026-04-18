from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark=SparkSession.builder.appName('temp').getOrCreate()

df=spark.createDataFrame(
    [('Aman',10),('chaman',20)],
    ['name','age']
)

df=df.withColumn('new_age',col('age')*2)
df.show()