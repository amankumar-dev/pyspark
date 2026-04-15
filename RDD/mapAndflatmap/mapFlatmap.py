from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())
rdd=spark.sparkContext.parallelize([1,2,3,4,5])

map_rdd=rdd.map(lambda x: [x,x*10])
flatMap_rdd=rdd.flatMap(lambda x: [x,x*10])

print(map_rdd.collect())
print(flatMap_rdd.collect())