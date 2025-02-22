from pyspark.sql import SparkSession
from delta import *

# Setup Spark + Delta Lake ( define DeltaSparkSessionExtension and DeltaCatalog)
spark = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
print("Spark + Delta Lake setup is successful!")

# Create DataFrame
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

print("DataFrame created successfully!", df)
