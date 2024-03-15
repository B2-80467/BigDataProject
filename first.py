import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import datetime
from pyspark.sql.functions import col , lit

# url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AXISBANK.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Data Transformation Example") \
    .getOrCreate()


list_of_dataframes = list()
def datashow(url):

    responce_from_website = requests.get(url)

    data = responce_from_website.json()





    # Define the schema for the DataFrame
    schema = StructType([
        StructField("Symbol", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", IntegerType(), True)
    ])

    # Provided data

    # Extract data and transform into DataFrame

    print(data)
    print("*"*80)
    try:
        time_series_data = data.get('Time Series (Daily)')
        rows = []
        for date, values in time_series_data.items():
            row = [
                data['Meta Data']['2. Symbol'],  # Adding symbol as a constant value
                date,
                float(values['1. open']),
                float(values['2. high']),
                float(values['3. low']),
                float(values['4. close']),
                int(values['5. volume'])
            ]
            rows.append(row)
    except:

        print(f"value error {url} " )


    # Create DataFrame from the extracted data

    try:
        df = spark.createDataFrame(rows, schema)

        ndf = df.select("*").where(col("Date") == f'{datetime.datetime.now().date().strftime("%Y-%m-%d")}')

        list_of_dataframes.append(ndf)
    except:
        print("error",url)
    # Show the DataFrame
    # df.show()
    # hundraddf = df.select("*").limit(100)
    # # hundraddf.write.format("orc").mode("overwrite").save("hdfs://localhost:9870/user/dbda/input")
    # hundraddf.write\
    #     .format("orc")\
    #     .option('path', 'hdfs://localhost:9000/user/dbda/input')\
    #     .mode('append')\
    #     .save()[]


# import requests


# now past the url for the fetch

axisBank = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AXISBANK.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
HDFCBANK = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=HDFCBANK.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
ICICIBANK = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=ICICIBANK.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"

MandM = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=M&M.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
MRF = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MRF.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
TVS = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TVSMOTOR.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"

## pharma
Cipla = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=Cipla.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
SunPharma = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SunPharma.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
Biocon = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=Biocon.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"

## FMCG

Patanjali = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=PATANJALI.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
Sula = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SULA.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
BajajHind = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=BAJAJHIND.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"

count = 0
for i in [axisBank,HDFCBANK,ICICIBANK,MandM,MRF,TVS,Cipla,SunPharma,Biocon,Patanjali,Sula,BajajHind]:
    print(count)
    count += 1

    datashow(i)

from functools import reduce
new_combined_dataFrame = reduce(lambda df1,df2: df1.union(df2) , list_of_dataframes)

### making directory in HDFS

# import os
# hadoop fs -mkdir -p

### HDFS Injestion
new_combined_dataFrame.write.mode('overwrite') \
    .option("header", "true") \
    .partitionBy("Symbol")\
    .format("csv")\
    .save("hdfs://localhost:9000/user/test/first")



