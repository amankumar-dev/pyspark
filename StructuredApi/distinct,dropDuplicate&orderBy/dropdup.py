from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('temp').getOrCreate()
df=spark.createDataFrame(
    [('aman','20'),('aman',"20"),('chaman','20')],
    ['name','age']
)

df.dropDuplicates(["name"]).show()          # sirf name column mai duplicate check karega