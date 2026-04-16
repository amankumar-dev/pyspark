from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('temp').getOrCreate()

df = spark.createDataFrame(
    [('something data...',)],   
    ['column']
)

df.write.csv('output_path', header=True)        # File should be exist
df.write.mode("overwrite").csv("output_path", header=True) # If not exist it will create it's own