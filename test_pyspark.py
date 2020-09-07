import time
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.master", "local") \
    .getOrCreate()

df = spark.read.load("hdfs://localhost:9000/test/mycsvjoin1.csv",format="csv", sep=":", inferSchema="true", header="true")
df = df.cache()


df1 = spark.read.load("hdfs://localhost:9000/test/mycsvnew1.csv",format="csv", sep=":", inferSchema="true", header="true")
df1 = df.cache()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
print(spark.conf.get("spark.sql.join.preferSortMergeJoin"))

t0 = time.time()
#df2 = df.join(df1,"column1","outer")
df2 = df.union(df1)
df2 = df2.cache()
t1 = time.time()
print(t1-t0)
