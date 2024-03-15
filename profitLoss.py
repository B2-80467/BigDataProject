import math

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

from  pyspark.sql.functions import col , lit

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Line Chart with PySpark") \
    .getOrCreate()

data = spark.read.format("csv").option("inferSchema",True).option("header",True).load("hdfs://localhost:9000/user/test/first")

df = data.withColumn("PL" , col("close") - col("open"))


df.createOrReplaceTempView("COMPANY")



COMPLETE = spark.sql("SELECT *, CASE WHEN Symbol = 'AXISBANK.BSE' THEN 1 WHEN Symbol = 'HDFCBANK.BSE' THEN 2 WHEN Symbol = 'ICICIBANK.BSE' then 3  WHEN Symbol = 'M&M.BSE' then 4  WHEN Symbol = 'MRF.BSE' then 5  WHEN Symbol = 'TVSMOTOR.BSE' then 6   WHEN Symbol = 'Cipla.BSE' then 7 WHEN Symbol = 'SunPharma.BSE' then 8  WHEN Symbol = 'Biocon.BSE' then 9 WHEN Symbol = 'PATANJALI.BSE' then 10  WHEN Symbol = 'SULA.BSE' then 11 WHEN Symbol = 'BAJAJHIND' then 12 END AS rank FROM COMPANY")

ndf = COMPLETE.createOrReplaceTempView("full")

finalDf =spark.sql("SELECT Date,Open,High,Low,Close,Symbol,PL FROM full WHERE rank in (1,2,3,4,5,6,7,8,9,10,11,12) ORDER BY Date DESC LIMIT 30 ")

finalDf.show(n=180)
# COMPLETE.show()



df.printSchema()


