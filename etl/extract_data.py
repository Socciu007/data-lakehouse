from pyspark.sql import SparkSession, DataFrame

# Extract data from a file
def extract_data(spark: SparkSession, path: str) -> DataFrame:
    try:
        # Check if the file extension is valid
        valid_extensions = ('.csv', '.jsonl', 'txt', '.xlsx', '.avro', '.orc', '.delta', '.parquet')
        if not path.endswith(valid_extensions):
            raise ValueError(f"Invalid file extension: {path}")
    
        # Read the data from the file
        if path.endswith('.csv'):
            df = spark.read.csv(path, header=True)
        elif path.endswith('.jsonl'):
            df = spark.read.json(path, multiLine=True)
        elif path.endswith('.txt'):
            df = spark.read.text(path)
        elif path.endswith('.xlsx'):
            df = spark.read.format("com.crealytics.spark.excel").option("header", True).load(path)
        elif path.endswith('.avro'):
            df = spark.read.format("avro").load(path)
        elif path.endswith('.orc'):
            df = spark.read.orc(path)
        elif path.endswith('.delta'):
            df = spark.read.format("delta").load(path)
        elif path.endswith('.parquet'):
            df = spark.read.parquet(path)
        else:
            raise ValueError(f"Unsupported file extension: {path}")

        return df
    except Exception as e:
        print(f"Error extracting data from {path}: {e}")
        raise e