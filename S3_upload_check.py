from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Upload to S3") \
    .getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv("part-00000-f264af3e-8bf5-4214-a773-90ebe34c6bab-c000.csv", header=True, inferSchema=True)

# Write the DataFrame to S3 as a CSV file
df.write.csv("s3://uploadchecks3/test/1/", header=True)

# Stop SparkSession
spark.stop()
