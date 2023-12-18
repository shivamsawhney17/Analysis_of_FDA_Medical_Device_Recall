import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
import random
from pyspark.sql.types import FloatType
import re
from etl.utils import get_object,load_object, data_from_warehouse
import pandas as pd
def spark_transform(**kwargs):
    date = kwargs['execution_date'].strftime('%Y-%m-%d')
    path = f'raw/{date}_open_fda_raw.csv'
    data = get_object(path)

    # data_source = 's3://emr-dcsc-s3/input_files/2023-12-12_open_fda_raw.csv'
    # output_uri = 's3://emr-dcsc-s3/output/transformed.csv'
    #
    # AWS_ACCESS_KEY_ID = 'AKIA46ITI7LD5SEQIE5W'
    # AWS_SECRET_ACCESS_KEY = '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'

    spark = SparkSession.builder.appName("transform").getOrCreate()
    df = spark.createDataFrame(data)
    print("Fetching data")
    #df = spark.read.csv(data_source, header=True)
    # Define a UDF (User Defined Function) to extract numeric values from text
    def extract_numeric_udf(text):
        if isinstance(text, str):
            # Use regular expression to find numeric values (including commas and dots)
            numeric_values = re.findall(r'\d[\d,]*(?:\.\d+)?', text)

            # Convert the extracted numbers to floats (considering commas)
            numeric_values = [float(value.replace(',', '')) for value in numeric_values]

            # Sum all numeric values
            total = sum(numeric_values)

            return total
        else:
            return None  # Return None for non-string or None values

    # Register the UDF with Spark
    extract_numeric_spark_udf = udf(extract_numeric_udf, FloatType())

    # Apply the UDF to the 'product_quantity' column and create a new 'product_quantity_cleaned' column
    df = df.withColumn("product_quantity_cleaned", extract_numeric_spark_udf(df["product_quantity"]))

    def cost_udf(val):
        if val < 1000:
            return val * random.randint(800, 1000)
        elif val < 10000:
            return val * random.randint(200, 500)
        elif 10000 < val < 100000:
            return val * random.randint(10, 20)
        elif val >= 100000:
            return val * random.randint(1, 5) * 0.01
        else:
            return None

    # Register the UDF with Spark
    cost_spark_udf = udf(cost_udf, FloatType())

    # Apply the UDF to the 'product_quantity_cleaned' column and create a new 'cost_to_recall_in_dollars' column
    df = df.withColumn("cost_to_recall_in_dollars", cost_spark_udf(df["product_quantity_cleaned"]))
    pd_df = df.toPandas()

    date = kwargs['execution_date'].strftime('%Y-%m-%d')
    load_object(pd_df, f'output/{date}_transformed.csv')
    # Show the result
    #df.write.mode('overwrite').csv(output_uri)

