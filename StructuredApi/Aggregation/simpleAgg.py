from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, min, count, countDistinct,when,col

spark=SparkSession.builder.appName('temp').getOrCreate()
df=spark.createDataFrame(
    [('Aman','IT',30000),('Chaman','IT',4000),('Khaman','Civil',20000),('Dhaman','Mech',4000)],
    ['name','dept','salary']
)

df.select(
    sum("salary"),
    avg("salary"),
    max("salary"),
    min("salary"),
    count("salary"),
    countDistinct("salary")
).show()
