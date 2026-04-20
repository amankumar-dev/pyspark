from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, min, count, countDistinct,when,col

spark=SparkSession.builder.appName('temp').getOrCreate()
df=spark.createDataFrame(
    [('Aman','IT',30000),('Chaman','IT',4000),('Khaman','Civil',20000),('Dhaman','Mech',4000)],
    ['name','dept','salary']
)

df.groupBy("dept").agg(
    sum("salary").alias("total_salary"),

    sum(
        when(col("salary") <= 4000, col("salary"))
    ).alias("low_salary_sum"),

    sum(
        when(col("salary") > 4000, col("salary"))
    ).alias("high_salary_sum")

).show()