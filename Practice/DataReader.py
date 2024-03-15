

from pyspark.sql import SparkSession

from pyspark.sql.functions import current_date

from pyspark.sql.functions import col, lit




# now build the spark secession

spark = SparkSession.builder.appName("datareader").getOrCreate()


















# AXISBANK = spark.read.option("inferSchema",True).option("header",True).csv("/home/sunbeam/Desktop/BIGDATAPROJECT/AXISBANK.csv")
#
# # now only hundrade
#
# axis = AXISBANK.select("*").limit(100)
# axis.show(5)
# print("*"*50)



# HDFCBANK = spark.read.option("inferSchema",True).option("header",True).csv("/home/sunbeam/Desktop/BIGDATAPROJECT/HDFCBABK.csv")
#
# hdfc = HDFCBANK.select("*").limit(100)
#
# hdfc.show(5)
# print("*"*50)

import sys
import datetime

# ICICIBANK = spark.read.option("inferSchema" , True).option("header" , True).csv("/home/sunbeam/Desktop/BIGDATAPROJECT/ICICIBANK.csv")
# icici = ICICIBANK.select("*").where(col("Date") == f'{datetime.datetime.now().date().strftime("%Y-%m-%d")}')
#
# icici.show(5)
# print("*"*50)
# print(datetime.datetime.now().date().strftime("%Y-%m-%d"))

# MandM = spark.read.option("inferSchema",True).option("header" , True).csv("/home/sunbeam/Desktop/BIGDATAPROJECT/MandM.csv")
# mm = MandM.select("*").limit(100)
# mm.show(5)
# print("*"*50)

# MRF = spark.read.option("inferSchema" , True).option("header" , True).csv("/home/sunbeam/Desktop/BIGDATAPROJECT/MRF.csv")
# mrf = MRF.select("Symbol","Date","Open","High","Low","Close","Volume")
# mrf.show(5)
# print("*"*50)


# print(current_date)

# TVSMOTOR = spark.read.option("inferSchema" , True).option("header",True).csv("/home/sunbeam/Desktop/BIGDATAPROJECT/TVSMOTOR.csv")
# tvs = TVSMOTOR.select("Symbol","Date","Open","High","Low","Close","Volume")
# tvs.show(5)
# print("*"*50)




from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window
import matplotlib.pyplot as plt





####### scatter ploat

import matplotlib.pyplot as plt
import pandas as pd

# Sample data
# data = {
#     'Symbol': ['MRF.BSE', 'MRF.BSE', 'MRF.BSE', 'MRF.BSE', 'MRF.BSE', 'TVSMOTOR.BSE', 'TVSMOTOR.BSE', 'TVSMOTOR.BSE', 'TVSMOTOR.BSE', 'TVSMOTOR.BSE'],
#     'Date': ['2024-03-07', '2024-03-06', '2024-03-05', '2024-03-04', '2024-03-01', '2024-03-07', '2024-03-06', '2024-03-05', '2024-03-04', '2024-03-01'],
#     'Open': [145400.0, 145375.0, 146499.9531, 145400.0469, 146800.0, 2289.75, 2299.55, 2249.6499, 2259.95, 2120.05],
#     'High': [145800.0, 146114.5, 147000.0, 146801.9063, 147216.2031, 2313.8999, 2299.55, 2290.0, 2266.95, 2280.0],
#     'Low': [143600.0, 144132.0, 145000.0, 144599.4531, 144267.0, 2254.1001, 2231.1499, 2232.2, 2220.25, 2120.05],
#     'Close': [143885.3438, 145903.7031, 145326.1563, 146150.0938, 145103.75, 2260.05, 2279.5, 2283.3501, 2232.7, 2242.45],
#     'Volume': [445.0, 327.0, 442.0, 442.0, 433.0, 16903.0, 99388.0, 31739.0, 26660.0, 55369.0]
# }

# Convert data to pandas dataframe
# df = pd.DataFrame(data)

# Create a scatter plot for different columns
# columns_to_plot = ['Open', 'High', 'Low', 'Close']
#
# for col in columns_to_plot:
#     plt.scatter(mrf['Date'], mrf.select(f'{col}'))
#     plt.xlabel('Date')
#     plt.ylabel(col)
#     plt.title(f'Scatter plot for {col} (MRF.BSE & TVSMOTOR.BSE)')
#     plt.grid(True)
#     plt.show()








#################

# Create a SparkSession

#### this is the moving average ==============================================================================
# Assume `df` is your DataFrame containing the stock data

# # Define the window specification for calculating moving averages
# window_spec_50 = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(7, 0)
# # window_spec_200 = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-200, 0)
#
# # Calculate moving averages (e.g., 50-day and 200-day)
# moving_avg_50 = avg("Close").over(window_spec_50).alias("MA_50")
# # moving_avg_200 = avg("Close").over(window_spec_200).alias("MA_200")
#
# # Add moving average columns to the DataFrame
# # df_with_ma = tvs.withColumn("MA_50", moving_avg_50).withColumn("MA_200", moving_avg_200)
# df_with_ma = tvs.withColumn("MA_50", moving_avg_50)
# # Convert Spark DataFrame to Pandas DataFrame for plotting
# pandas_df = df_with_ma.toPandas()

# print(pandas_df.)

# Plot the trends for each stock symbol
# for symbol, group in pandas_df.groupby("Symbol"):
#     plt.plot(group["Date"], group["Close"], label=symbol)
#     plt.plot(group["Date"], group["MA_50"], label="50-day MA")
#     # plt.plot(group["Date"], group["MA_200"], label="200-day MA")
#     plt.xlabel("Date")
#     plt.ylabel("Price")
#     plt.title("Stock Price and Moving Averages")
#     plt.legend()
#     plt.show()

# Stop SparkSession


### this is the moving average ==============================================================================





# Create a SparkSession


# Assume `df` is your DataFrame containing the stock data

# Group the data by date and calculate the average volume for each date
# volume_trend = tvs.groupBy("Date").agg(avg("Volume").alias("AverageVolume"))
#
# # Convert Spark DataFrame to Pandas DataFrame for plotting
# pandas_volume_trend = volume_trend.toPandas()
#
# # print(pandas_volume_trend)
#
# # Plot the volume trend over time
# plt.plot(pandas_volume_trend["Date"], pandas_volume_trend["AverageVolume"])
# plt.xlabel("Date")
# plt.ylabel("Average Volume")
# plt.title("Volume Trend Over Time")
# plt.show()

# Stop SparkSession


############ now i am doing the correlation for different companies

from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import seaborn as sns
import matplotlib.pyplot as plt



# Assume `mrf` and `tvs` are your DataFrames containing the stock data for MRF and TVSMOTOR

# Assemble the features into a single vector column for MRF
# assembler_mrf = VectorAssembler(inputCols=["Open", "High", "Low", "Close", "Volume"], outputCol="features_mrf")
# assembled_mrf_df = assembler_mrf.transform(mrf).select("features_mrf")
#
# # Assemble the features into a single vector column for TVSMOTOR
# assembler_tvs = VectorAssembler(inputCols=["Open", "High", "Low", "Close", "Volume"], outputCol="features_tvs")
# assembled_tvs_df = assembler_tvs.transform(tvs).select("features_tvs")
#
# # Calculate the correlation matrix between MRF and TVSMOTOR
# correlation_matrix = Correlation.corr(assembled_mrf_df.crossJoin(assembled_tvs_df), "features_mrf", method="pearson").head()
# correlation_array = correlation_matrix[0].toArray()
#
# # Convert the correlation matrix to a Pandas DataFrame for visualization
# correlation_df = spark.createDataFrame(correlation_array.tolist(), ["Open", "High", "Low", "Close", "Volume"])
# correlation_pandas_df = correlation_df.toPandas()
#
# print(correlation_pandas_df)

# Visualize the correlation matrix
# plt.figure(figsize=(10, 8))
# sns.heatmap(correlation_pandas_df, annot=True, cmap="coolwarm", fmt=".2f", square=True)
# plt.title("Correlation Matrix between MRF and TVSMOTOR")
# plt.xlabel("MRF")
# plt.ylabel("TVSMOTOR")
# plt.show()

# Stop SparkSession


# ==========================================================================================================


# import matplotlib.pyplot as plt

# Plotting function
# def plot_trend_chart(company_name, company_df):
#     plt.figure(figsize=(10, 6))
#     plt.plot(company_df["Date"], company_df["Open"], label="Open")
#     plt.plot(company_df["Date"], company_df["Close"], label="Close")
#     plt.xlabel("Date")
#     plt.ylabel("Price")
#     plt.title(f"Trend Chart for {company_name}")
#     plt.legend()
#     plt.xticks(rotation=45)
#     plt.grid(True)
#     plt.show()
#
# # Draw trend chart for MRF
# plot_trend_chart("MRF", mrf)
#
# # Draw trend chart for TVSMOTOR
# plot_trend_chart("TVSMOTOR", tvs)




