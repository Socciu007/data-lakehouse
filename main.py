from pyspark.sql import SparkSession
from delta import *
from etl.extract_data import extract_data

# Setup Spark + Delta Lake ( define DeltaSparkSessionExtension and DeltaCatalog)
spark = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
print("Spark + Delta Lake setup is successful!")

df = spark.read.csv('data/tourism_dataset.csv', header=True)

# Create DataFrame
# data = extract_data(spark, "../data/tourism_dataset.csv")
print(df.show())

print("DataFrame created successfully!")