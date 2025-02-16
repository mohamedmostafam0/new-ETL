from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
sys.path.insert(0, '/opt/bitnami/spark/scripts')  # Ensure Spark finds constants.py
from constants import (
    KAFKA_BROKER, TOPIC, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD
)

def define_schema():
    """Define schema for incoming Kafka JSON data."""
    return StructType() \
        .add("InvoiceNo", StringType()) \
        .add("StockCode", StringType()) \
        .add("Description", StringType()) \
        .add("Quantity", IntegerType()) \
        .add("InvoiceDate", StringType()) \
        .add("UnitPrice", FloatType()) \
        .add("CustomerID", StringType()) \
        .add("Country", StringType())

def initialize_spark():
    """Initialize and return a Spark session."""
    return SparkSession.builder \
        .appName("SalesDataPipeline") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.1") \
        .getOrCreate()

def read_from_kafka(spark):
    """Read streaming data from Kafka topic."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC) \
        .load()

def parse_kafka_data(df, schema):
    """Parse Kafka JSON messages into structured DataFrame."""
    return df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

def write_to_postgres(df, epoch_id):
    """Write batch data to PostgreSQL."""
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "sales") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def main():
    """Main function to execute the streaming pipeline."""
    schema = define_schema()
    spark = initialize_spark()
    kafka_df = read_from_kafka(spark)
    parsed_df = parse_kafka_data(kafka_df, schema)
    
    query = parsed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()