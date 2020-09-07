import time
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SQLContext



spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.master", "local") \
    .getOrCreate()

#df = spark.read.load("hdfs://localhost:9000/test/mycsvjoin1.csv",format="csv",sep=":", inferSchema="true", header="true")
#df1 = spark.read.load("hdfs://localhost:9000/test/mycsvnew1.csv",format="csv", sep=":", inferSchema="true", header="true")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
print(spark.conf.get("spark.sql.join.preferSortMergeJoin"))
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://localhost:9000/test/mycsvjoin1.csv")
df1 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://localhost:9000/test/mycsvnew1.csv")


df = df.cache()
print(df.show())


df1 = df1.cache()
print(df1.show())

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
print(spark.conf.get("spark.sql.join.preferSortMergeJoin"))

t0 = time.time()
#df2 = df.join(df1, on=['column1'], how='inner')
df2 = df.union(df1).distinct()
#df2 = df2.cache()
t1 = time.time()
print(df2.show())
print(t1-t0)

