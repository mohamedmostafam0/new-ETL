from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

KAFKA_BROKER = "localhost:9094"
TOPIC = "sales_data"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/${POSTGRES_DB}"
POSTGRES_USER = ${POSTGRES_USER}
POSTGRES_PASSWORD =${POSTGRES_PASSWORD}


# Define schema for incoming data
schema = StructType() \
    .add("InvoiceNo", StringType()) \
    .add("StockCode", StringType()) \
    .add("Description", StringType()) \
    .add("Quantity", IntegerType()) \
    .add("InvoiceDate", StringType()) \
    .add("UnitPrice", FloatType()) \
    .add("CustomerID", StringType()) \
    .add("Country", StringType())

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SalesDataPipeline") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.1") \
    .getOrCreate()

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .load()

# Convert Kafka JSON data to structured format
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write stream to PostgreSQL
def write_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "sales") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
