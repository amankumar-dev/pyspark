from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark=(SparkSession.builder.appName('temp').getOrCreate())

schema=StructType([
    StructField('name',StringType(),True),
    StructField('id',IntegerType(),True)
])

df=spark.createDataFrame(
    [('aman',10),('chaman',20),('dhaman',30)],
    schema
).show()