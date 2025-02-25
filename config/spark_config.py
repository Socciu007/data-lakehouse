from pyspark.sql import SparkSession

# Config Spark Session
def setup_spark_session(app_name="DeltaLakeApp"):
    spark = (SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    return spark
