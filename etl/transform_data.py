from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col

def clean_data(df: DataFrame) -> DataFrame:
    # Remove rows with null values
    df_clean = df.dropna()
    return df_clean

def convert_date(df: DataFrame) -> DataFrame:
    # Convert pickup_datetime to date
    df_transformed = df.withColumn("pickup_date", to_date(col["pickup_datetime"]))
    return df_transformed

def format_column_name(df: DataFrame) -> DataFrame:
    # Get list of column names and remove spaces and convert to lowercase
    new_columns = [col.strip().replace(" ", "_") for col in df.columns]

    # Set new column names for DataFrame
    df = df.toDF(*new_columns)
    return df