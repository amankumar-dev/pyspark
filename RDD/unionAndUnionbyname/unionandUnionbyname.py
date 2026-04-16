from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())
rdd1=spark.sparkContext.parallelize([1,2,3,4,3,2])
rdd2=spark.sparkContext.parallelize([5,6,5,6,7])

df1=spark.createDataFrame([(1,'aman')],['id','name'])
df2=spark.createDataFrame([('chaman',2)],['name','id'])

unionRdd=rdd1.union(rdd2)                               # Join two rdd/df into one
unionbyname=df1.unionByName(df2)                        # Join two df into one on the basis of column and value

print(unionRdd.collect())
print(unionbyname.collect())