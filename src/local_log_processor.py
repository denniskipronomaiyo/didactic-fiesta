from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# --- Local Paths ---
SOURCE_PATH = "./logs/"
DEST_PATH = "./processed-logs/"
CHECKPOINT_PATH = "./checkpoints/"

# --- Spark Session Setup ---
def create_spark_session():
    return (
        SparkSession.builder
        .appName("LocalRealTimeLogProcessor")
        .getOrCreate()
    )

# --- Log Processing ---
def process_logs():
    spark = create_spark_session()

    log_schema = StructType([
        StructField("raw", StringType(), True),  # raw log line
    ])

    print(f"Watching directory: {SOURCE_PATH}...")

    df = spark.readStream.format("text").schema(log_schema).load(SOURCE_PATH)

    processed_df = (
        df.withColumn("timestamp", regexp_extract(col("raw"), "\\[(.*?)\\]", 1))
          .withColumn("level", regexp_extract(col("raw"), "\\[(.*?)\\]\\s*\\[(.*?)\\]", 2))
          .withColumn("message", regexp_extract(col("raw"), "\\]\\s*(.*)", 1))
          .withColumn("ingested_at", current_timestamp())
          .drop("raw")
    )

    query = (
        processed_df.writeStream
        .format("parquet")
        .option("path", DEST_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )

    print("Stream started. Press Ctrl+C to stop.")
    query.awaitTermination()

if __name__ == "__main__":
    process_logs()
