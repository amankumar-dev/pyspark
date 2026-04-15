from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())
rdd=spark.sparkContext.parallelize([
    ('A',3),
    ('D',2),
    ('C',5)
])

sortBy_rdd=rdd.sortBy(lambda x: x[1])
sortByKey_rdd=rdd.sortByKey()

print(sortBy_rdd.collect())
print(sortByKey_rdd.collect())