from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,expr

spark=SparkSession.builder.appName('temp').getOrCreate()
df=spark.createDataFrame(
    [(1,'Alice',5000),(2,'Aman',None),(3,None,3000),(4,'David',-100),(5,'Eve','abc')],
    ['id','name','salary']
)

# Handling Messing values(Nulls)
df.dropna().show()                          # Remove all Rows which has null
df.fillna({'name':'Unknown','salary':0}).show()    # Fill values who have null

# Fixing bad datatypes
df.withColumn(
    "salary",
    expr("try_cast(salary as int)")
)

# Handling Invalid values
df.withColumn(
    'salary',
    when(col('salary')<0,None).otherwise(col('salary'))
).show()

# Drop Duplicates
df.dropDuplicates().show()

# Filtering clean data
df.filter(col('salary').isNotNull()).show()