from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,expr,udf
from pyspark.sql.types import StringType


spark=SparkSession.builder.appName('temp').getOrCreate()
df=spark.createDataFrame(
    [(1,'Alice',5000),(2,'Aman',None),(3,None,3000),(4,'David',-100),(5,'Eve','abc')],
    ['id','name','salary']
)

def to_upper(name):
    return name.upper() if name else None

upperDf=udf(to_upper,StringType())

df=df.withColumn('upper_name',upperDf(df['name']))
df.show()