from pyspark.sql import DataFrame

# Load data to delta table
def load_data(df: DataFrame, path: str):
    df.write.mode("overwrite").save(path)