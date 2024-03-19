from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def fetch_api_data():
    import requests
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
    import datetime
    from pyspark.sql.functions import col, lit
    from functools import reduce

    # url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AXISBANK.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Data Transformation Example") \
        .getOrCreate()

    list_of_dataframes = list()

    def datashow(url):
        response_from_website = requests.get(url)
        data = response_from_website.json()  # data in object form so converting into json to see

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
            # c_df = df.select("*").where(col("Date") == f'{datetime.datetime.now().date().strftime("%Y-%m-%d")}')  # df of today's date
            # this is for current date so fisrt ingest all data then append the current date wala
            list_of_dataframes.append(df)
        except:
            print("error", url)

    # BANKING
    axisBank = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AXISBANK.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"
    HDFCBANK = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=HDFCBANK.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"
    ICICIBANK = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=ICICIBANK.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"

    # AUTOMOBILE
    MandM = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=M&M.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"
    MRF = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MRF.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"
    TVS = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TVSMOTOR.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"

    # PHARMA
    Cipla = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=Cipla.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"
    SunPharma = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SunPharma.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"
    Biocon = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=Biocon.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"

    # FMCG
    Patanjali = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=PATANJALI.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"
    Sula = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SULA.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"
    BajajHind = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=BAJAJHIND.BSE&outputsize=full&apikey=25N2A6DGPRJ0T4QR"

    # ,ICICIBANK,MandM,MRF,TVS,Cipla,SunPharma,Biocon,Patanjali,Sula,BajajHind
    count = 0
    for company in [axisBank, HDFCBANK]:
        print(count)  # just to check
        count += 1
        datashow(company)  # function call

    # combing df's
    combined_dataFrame = reduce(lambda df1, df2: df1.union(df2), list_of_dataframes)

    # combined_dataFrame.show(n=150)
    ## making directory in HDFS
    # hadoop fs -mkdir -p your path
    # hadoop fs -rm -R dir    -> to remove the dir or file
    # df = spark.read.csv('MandM.csv/part-00001-3b613bc9-c0fb-4f2c-88f5-dac2f5db2933-c000.csv')

    ### HDFS Injestion
    combined_dataFrame.write.mode('overwrite') \
        .option("header", "true") \
        .partitionBy('Symbol') \
        .format("csv") \
        .save("hdfs://localhost:9000/user/airflow/test/first")

def business_requirement():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit

    # Create  SparkSession
    spark = SparkSession.builder \
        .appName("Line Chart with PySpark") \
        .getOrCreate()

    data = spark.read.format("csv") \
        .option("inferSchema", True) \
        .option("header", True) \
        .load("hdfs://localhost:9000/user/airflow/test/first")

    df = data.withColumn("PL", col("close") - col("open"))  # added profit/loss column
    df.createOrReplaceTempView("table1")  # table created

    # creating table with rank to each company
    table1 = spark.sql(
        "SELECT *, CASE WHEN Symbol = 'AXISBANK.BSE' THEN 1 WHEN Symbol = 'HDFCBANK.BSE' THEN 2 WHEN Symbol = 'ICICIBANK.BSE' then 3  WHEN Symbol = 'M&M.BSE' then 4  WHEN Symbol = 'MRF.BSE' then 5  WHEN Symbol = 'TVSMOTOR.BSE' then 6   WHEN Symbol = 'Cipla.BSE' then 7 WHEN Symbol = 'SunPharma.BSE' then 8  WHEN Symbol = 'Biocon.BSE' then 9  WHEN Symbol = 'BAJAJHIND' then 10 END AS rank FROM COMPANY")

    table2 = table1.createOrReplaceTempView("full")  # table created

    finalDF = spark.sql(
        "SELECT Date,Open,High,Low,Close,Symbol,PL  FROM full WHERE rank in (1,2,3,4,5,6,7,8,9,10) ORDER BY Date DESC LIMIT 150 ")

    finalDF.show(n=180)
    finalDF.write.mode('overwrite') \
        .option("header", "true") \
        .format("csv") \
        .save("/home/sunbeam/Documents/project_output/final.csv")

dag_arg = {
    'owner':'ayush',
    'retries': '2',
    'retry_delay' : timedelta(minutes=2)
}

with DAG (
    dag_id= 'test_15',
    default_args= dag_arg,
    schedule_interval= '@once',
    start_date= datetime(2024, 3, 18),
    catchup = True
) as dag:

    tast1 = PythonOperator(
        task_id = 'fetching_api_data_and_clean_1',
        python_callable= fetch_api_data
    )

    task2 = PythonOperator(
        task_id='business_requirement_1',
        python_callable= business_requirement
    )






tast1 >> task2