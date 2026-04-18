from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark=SparkSession.builder.appName('temp').getOrCreate()
df=spark.createDataFrame(
    [('aman','40'),('aman',"20"),('chaman','30')],
    ['name','age']
)

df.orderBy('age').show()
df.orderBy(col('age').desc()).show()