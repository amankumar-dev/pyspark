from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())
rdd=spark.sparkContext.parallelize([1,2,1,3,4,4,5])

unique_rdd=rdd.distinct()                           # Distinct use to remove duplicate data
even_rdd=rdd.filter(lambda x: x%2==0)               # Filter just store condition met data

print(unique_rdd.collect())
print(even_rdd.collect())