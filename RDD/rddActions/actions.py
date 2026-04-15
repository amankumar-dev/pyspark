from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())
rdd=spark.sparkContext.parallelize([1,2,3,4,5,4])

print(rdd.collect())
print(rdd.count())
print(rdd.first())
print(rdd.take(3))
print(rdd.reduce(lambda x,y:x+y))