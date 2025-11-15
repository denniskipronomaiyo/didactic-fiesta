from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# --- Configuration ---
SOURCE_S3_PATH = "s3a://your-source-bucket/logs/"
DEST_S3_PATH = "s3a://your-destination-bucket/processed-logs/"

# --- Spark Session Setup ---
def create_spark_session():
    return (
        SparkSession.builder
        .appName("RealTimeLogProcessor")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .getOrCreate()
    )

# --- Log Parsing Logic ---
def process_logs():
    spark = create_spark_session()

    # Define log format: [2025-11-14 19:22:30] [INFO] message
    log_schema = StructType([
        StructField("raw", StringType(), True),  # entire raw log line
    ])

    df = spark.readStream.format("text").schema(log_schema).load(SOURCE_S3_PATH)

    # Extract timestamp, level, message
    processed_df = (
        df.withColumn("timestamp", regexp_extract(col("raw"), "\\[(.*?)\\]", 1))
          .withColumn("level", regexp_extract(col("raw"), "\\[(.*?)\\]\\s*\\[(.*?)\\]", 2))
          .withColumn("message", regexp_extract(col("raw"), "\\]\\s*(.*)", 1))
          .withColumn("ingested_at", current_timestamp())
          .drop("raw")
    )

    # Write out to S3 in parquet format
    query = (
        processed_df.writeStream
        .format("parquet")
        .option("path", DEST_S3_PATH)
        .option("checkpointLocation", "s3a://your-destination-bucket/checkpoints/")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    process_logs()
