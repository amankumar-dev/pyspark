# An operation that create as new df/RDD but doesn't execute immediately (Transformation)
# An operation that actually triggers execution (Actions)

from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())
data=r'D:\Aman\aman.code\pySpark\sparkExecution\user_data.csv';
df=(spark.read.format('csv').option('header','true').option('inferSchema','true').load(data))
df.select('user_id','username','city').show()           # Transformation , Actions
