from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Reading From HDFS").getOrCreate()

# reading from HDFS
data = spark.read.format("csv")\
        .option("inferSchema", True)\
        .option("header", True)\
        .load("hdfs://localhost:9000/user/test/first")


data.show()
