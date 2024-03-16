import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import datetime
from pyspark.sql.functions import col , lit

# url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AXISBANK.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Data Transformation Example") \
    .getOrCreate()


df = spark.read.csv('MandM.csv/part-00000-3b613bc9-c0fb-4f2c-88f5-dac2f5db2933-c000.csv')
df.show(n=50)

df.write.mode('append') \
    .option("header", "true") \
        .format("csv") \
    .save("hdfs://localhost:9000/user/test/second")
