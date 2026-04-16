from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName('temp').getOrCreate())

df=spark.createDataFrame(
    [(1,'aman',3000),(2,'chaman',15000)],
    ['id','name','salary']
)

df.createOrReplaceTempView('employees')
result=spark.sql('''
                 SELECT name,salary
                 FROM employees
                 WHERE salary>10000''')
print(result.collect())