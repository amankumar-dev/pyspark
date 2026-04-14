# RDD is a distributed collection of data that can be processed in parallel.

from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())
word_list="my name is aman and aman is very smart and aman can do anything in this world he can achieve anything".split(" ")

word_rdd=spark.sparkContext.parallelize(word_list)      # Convert python list into rdd data
word_data=word_rdd.collect()            # Convert rdd data into pythong list

for word in word_data:
    print(word)
