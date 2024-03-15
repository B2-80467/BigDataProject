from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import requests
import json
urlAXISBANK = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AXISBANK.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
urlArvind = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=Arvind.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
URLSKFINDIA = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SKFINDIA.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
URLBATA = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=BATAINDIA.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
urlIRCTC = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IRCTC.BSE&outputsize=full&apikey=Z96HU6A4EZ3KBB8V"
allURL = [urlArvind, URLSKFINDIA ,URLBATA, urlIRCTC]
#


# here read data from multiple companies and store it locally

dataFramesOftenCompanies = []

def extractorAndTransformer(urlModifed):

    responce_from_website = requests.get(urlModifed)
    data = responce_from_website.json()
    # print(data)

    # now make a spark secession for the spark

    spark = SparkSession.builder.appName("dataframapis").getOrCreate()
    # now above i builded the spark secession

    # now read the schema from the data
    schema = StructType([
        StructField("Symbol", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", IntegerType(), True)
    ])

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

    # now i will create the dataframe
    df = spark.createDataFrame(rows, schema)

    print(df.head(100))

    # now i will append that datafram into the dataFramesOfsixCompanies

    dataFramesOftenCompanies.append(df)









for url in allURL:
    extractorAndTransformer(url)
    break


print(len(dataFramesOftenCompanies))






# in second step write that data into hdfs partation wise this will make you





