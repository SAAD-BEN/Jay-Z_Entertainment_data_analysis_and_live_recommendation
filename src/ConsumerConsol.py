import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for the JSON data
schema = StructType([
    StructField("movie", StructType([
        StructField("genres", StringType(), True),
        StructField("movieId", StringType(), True),
        StructField("title", StringType(), True)
    ]), True),
    StructField("rating", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user", StructType([
        StructField("age", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("userId", StringType(), True)
    ]), True)
])

# Create a Spark session
spark = SparkSession.builder \
    .appName("UserProfileAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# Subscribe to the Kafka topic
topic = 'reviews'
df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .load()

# Convert the value column from binary to string
value = df.selectExpr("CAST(value AS STRING)")

# Apply schema to the JSON data
schema = value.select(from_json(col("value"), schema).alias("value"))

# Select the individual columns
cleaned_df = schema.select("value.*")

# Get the desired fields from the nested structure
nested_df = cleaned_df.select(col("movie.*"), col("rating"), col("user.*"))

streaming_es_query = nested_df.writeStream.outputMode("append").format("console").option("format", "json").start()
streaming_es_query.awaitTermination()