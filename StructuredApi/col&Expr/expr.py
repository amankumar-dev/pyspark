from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark=SparkSession.builder.appName('temp').getOrCreate()
df=spark.createDataFrame(
    [('aman',10),('chaman',20)],
    ['name','age']
)

df.select(
    expr('name'),
    expr('age * 2 as new_age')
).show()