import json
import re

# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName("praticrc").getOrCreate()
#
#
# data = spark.read.text("/home/sunbeam/Desktop/BIGDATAPROJECT/HDFS_BANK")




# import json
#
# # Read the text file line by line
# with open("/home/sunbeam/Desktop/BIGDATAPROJECT/HDFS_BANK", "r") as file:
#     lines = file.readlines()
#
# # Initialize an empty dictionary to store the parsed JSON data
# parsed_data = {}
#
# # Iterate through each line and parse it as JSON
# for line in lines:
#     try:
#         data = json.loads(line)
#         parsed_data.update(data)
#     except json.JSONDecodeError:
#         print("Error parsing JSON:", line)

# Now `parsed_data` contains the entire parsed JSON data from the text file
# You can access specific sections as needed










# data = 0
# with open("/home/sunbeam/Desktop/BIGDATAPROJECT/HDFS_BANK" , "r") as file:
#
#     data = file.read()
#
#     print(data)
#
#     # print(data)
# with open("NewHDFS.json" , "w" ) as file:
#
#     file.write(json.dumps(data))



# import json
#
# with open("NewHDFS.json", "r") as file:
#     data = file.read()
#     print("Data read from file:", data)  # Debugging line
#     newdata = json.loads(data)
#     print("Type of newdata:", type(newdata))  # Debugging line
#     time_series_daily = newdata["Time Series (Daily)"]


# with open('time_series_data.json', 'w') as json_file:
#     json.dump( data,"/home/sunbeam/Desktop/BIGDATAPROJECT/Practice", indent=4)












import requests


import json

# Specify the file path where the JSON data is stored
file_path = "/home/sunbeam/Desktop/BIGDATAPROJECT/tvsmotor.json"

# Read the JSON data from the file
with open(file_path, "r") as file:
    data = json.load(file)

# Now `data` contains the JSON data read from the file
# You can access and manipulate the data as needed



# now i have to make the schema
rows = []
for date , values in data.get("Time Series (Daily)").items():

    row = [

        data['Meta Data']['2. Symbol'],
        date,
        float(values['1. open']),
        float(values['2. high']),
        float(values['3. low']),
        float(values['4. close']),
        float(values['5. volume'])

    ]
    rows.append(row)


from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# now make a schema
schema = StructType([
        StructField("Symbol", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", DoubleType(), True)
    ])




# now build the spark secession

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("datastructure").getOrCreate()

df = spark.createDataFrame(rows , schema)



df.show()

df.write.csv("/home/sunbeam/Desktop/BIGDATAPROJECT/TVSMOTOR.csv", header=True)



















