from pyspark.sql import SparkSession, DataFrame

def extract_data(spark: SparkSession, path: str) -> DataFrame:
    # Check if the file extension is valid
    valid_extensions = ('.csv', '.jsonl', 'txt', '.xlsx', '.avro', '.orc', '.delta')
    if not path.endswith(valid_extensions):
        raise ValueError(f"Invalid file extension: {path}")
    
    # Read the data from the file
    if path.endswith('.csv'):
        df = spark.read.csv(path, header=True, inferSchema=True)
    elif path.endswith('.jsonl'):
        df = spark.read.json(path, multiLine=True)
    elif path.endswith('.txt'):
        df = spark.read.text(path)
    elif path.endswith('.xlsx'):
        df = spark.read.format("com.crealytics.spark.excel").option("header", True).load(path)
    elif path.endswith('.avro'):
        df = spark.read.format("avro").load(path)
    elif path.endswith('.orc'):
        df = spark.read.format("orc").load(path)
    elif path.endswith('.delta'):
        df = spark.read.format("delta").load(path)
    else:
        raise ValueError(f"Unsupported file extension: {path}")

    return df
