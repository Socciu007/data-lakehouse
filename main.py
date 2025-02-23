from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("DeltaLakeApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
print("Spark + Delta Lake setup is successful!", spark)

# Tạo DataFrame mẫu
data = [(1, "Alice"), (2, "Bob")]
df = spark.createDataFrame(data, ["id", "name"])

# Hiển thị DataFrame
df.show()
