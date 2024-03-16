import math
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
        .appName("Line Chart with PySpark") \
        .getOrCreate()

data = spark.read.format("csv").option("inferSchema", True).option("header", True).load(
    "hdfs://localhost:9000/user/test/first")

price_df = data.select("*").limit(10)

# Calculate daily returns
price_df = price_df.withColumn("daily_return", (col("high") - col("low")) / col("low") * 100)

# Calculate standard deviation of daily returns (volatility)
# volatility = price_df.agg({"daily_return": "stddev"}).collect()[0][0]

# Plotting
dates = price_df.select("date").collect()
daily_return = price_df.select("daily_return").collect()

print("*" * 80)
print(dates)
print(daily_return)
print("*" * 80)

# plt.figure(figsize=(10, 6))
# plt.scatter(dates, daily_return , label='Volatility%')
# plt.title('Date vs Volatility')
# plt.xlabel('Date')
# plt.ylabel('Volatility%')
# plt.legend()
# plt.grid(True)
# plt.show()

# Stop the SparkSession
spark.stop()
