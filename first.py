import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import datetime
from pyspark.sql.functions import col, lit
from functools import reduce

# url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AXISBANK.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"
# Create SparkSession
spark = SparkSession.builder \
    .appName("Data Transformation Example") \
    .getOrCreate()

list_of_dataframes = list()

def datashow(url):
    response_from_website = requests.get(url)
    data = response_from_website.json()              # data in object form so converting into json to see

    # Defining schema of DataFrame
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
    print()
    print("*" * 80)

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
        print(f"value error {url} ")

    # Create DataFrame from the extracted data
    try:
        df = spark.createDataFrame(rows, schema)
        # c_df = df.select("*").where(col("Date") == f'{datetime.datetime.now().date().strftime("%Y-%m-%d")}')     #df of today's date
        list_of_dataframes.append(df)
    except:
        print("error", url)


# BANKING
axisBank = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AXISBANK.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"
HDFCBANK = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=HDFCBANK.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"
ICICIBANK = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=ICICIBANK.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"

#AUTOMOBILE
MandM = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=M&M.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"
MRF = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MRF.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"
TVS = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TVSMOTOR.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"

# PHARMA
Cipla = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=Cipla.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"
SunPharma = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SunPharma.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"
Biocon = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=Biocon.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"

# FMCG
Patanjali = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=PATANJALI.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"
Sula = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SULA.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"
BajajHind = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=BAJAJHIND.BSE&outputsize=full&apikey=00ENS2T9SC25PHKO"

# ,ICICIBANK,MandM,MRF,TVS,Cipla,SunPharma,Biocon,Patanjali,Sula,BajajHind
count = 0
for company in [axisBank, HDFCBANK]:

    print(count)                      #just to check
    count += 1
    datashow(company)                 #function call

#combing df's
combined_dataFrame = reduce(lambda df1, df2: df1.union(df2), list_of_dataframes)

### making directory in HDFS
# hadoop fs -mkdir -p your path
# hadoop -rm -R dir    -> to remove the dir or file
# df = spark.read.csv('MandM.csv/part-00001-3b613bc9-c0fb-4f2c-88f5-dac2f5db2933-c000.csv')

### HDFS Injestion
combined_dataFrame.write.mode('append') \
    .option("header", "true") \
    .partitionBy('Symbol')\
    .format("csv") \
    .save("hdfs://localhost:9000/user/test")
