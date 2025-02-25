from config.spark_config import setup_spark_session

# Setup Spark Session
spark = setup_spark_session()
print("Spark + Delta Lake setup is successful!")

# Create Sample DataFrame
data = [[1, "Alice"], [2, "Bob"]]
df = spark.createDataFrame(data, ["id", "name"])

# Display DataFrame
df.show()

# Save DataFrame to Delta Table
# df.write.format("delta").mode("overwrite").save("data/delta_table/users")

# Read Delta Table
# df_delta = spark.read.format("delta").load("data/delta_table/users")
# df_delta.show()
