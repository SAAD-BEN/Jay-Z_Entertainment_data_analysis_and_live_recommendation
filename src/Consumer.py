import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws, udf, to_date, from_unixtime, date_format
from pyspark.sql.types import StructType, StructField, StringType,  FloatType, IntegerType, ArrayType
from dotenv import load_dotenv
import os
import json
from elasticsearch import Elasticsearch
from typing import Dict

# Load environment variables
load_dotenv()

# Function to create an Elasticsearch index if it doesn't exist
def create_index( index_name: str, mapping: Dict):
    try:
        elastic_client = Elasticsearch(
            os.getenv("ELASTIC_URL"),
            api_key=(os.getenv("ELASTIC_API_KEY"))
        )
        # Use Elasticsearch.indices.create method
        elastic_client.indices.create(
            index=index_name,
            body=mapping,
            ignore=400  # ignore 400 already exists code
        )
        print(f"Created index {index_name} successfully!")

        # close the Elasticsearch connection
        elastic_client.close()
        return True
    except Exception as e:
        print(f"Error creating index {index_name}: {str(e)}")
        return False 
    
# Function to find a movie rating count by movieId
def find_movie_rating_count_by_id(movie_id: int):
    try:
        elastic_client = Elasticsearch(
            os.getenv("ELASTIC_URL"),
            api_key=(os.getenv("ELASTIC_API_KEY"))
        )
        # Use Elasticsearch.search method
        response = elastic_client.search(
            index="jayzz_movie_index",
            body={
                "query": {
                    "match": {
                        "movieId": movie_id
                    }
                }
            }
        )
        # close the Elasticsearch connection
        elastic_client.close()
        return int(response["hits"]["hits"][0]["_source"]["rating_count"])
    except Exception as e:
        return int(0)
    
# Function to find a movie rating average by movieId
def find_movie_avg_rating_by_id(movie_id: int):
    try:
        elastic_client = Elasticsearch(
            os.getenv("ELASTIC_URL"),
            api_key=(os.getenv("ELASTIC_API_KEY"))
        )
        # Use Elasticsearch.search method
        response = elastic_client.search(
            index="jayzz_movie_index",
            body={
                "query": {
                    "match": {
                        "movieId": movie_id
                    }
                }
            }
        )
        # close the Elasticsearch connection
        elastic_client.close()
        return float(response["hits"]["hits"][0]["_source"]["rating_avg"])
    except Exception as e:
        return float(0)

# Function to find user activity_count by userId
def find_user_activity_count_by_id(user_id: int):
    try:
        elastic_client = Elasticsearch(
            os.getenv("ELASTIC_URL"),
            api_key=(os.getenv("ELASTIC_API_KEY"))
        )
        # Use Elasticsearch.search method
        response = elastic_client.search(
            index="jayzz_user_index",
            body={
                "query": {
                    "match": {
                        "userId": user_id
                    }
                }
            }
        )
        # close the Elasticsearch connection
        elastic_client.close()
        return int(response["hits"]["hits"][0]["_source"]["activity_count"])
    except Exception as e:
        return int(0)

# Function to initialize a Spark session
def initialize_spark():
    return SparkSession.builder \
        .appName("UserProfileAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"\
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

# Function to read data from Kafka topic and return a DataFrame
def read_from_kafka(spark, kafka_bootstrap_servers, kafka_topic):
    return (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

# Function to parse JSON data from Kafka message
def parse_kafka_message(data, schema):
    return (
        data
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json("json", schema).alias("data"))
        .select("data.*")
    )

# Function to write DataFrame to Elasticsearch index
def write_to_elasticsearch(data, index_name, elastic_settings, document_id_column):
    query = (
        data
        .writeStream
        .format("org.elasticsearch.spark.sql")
        .outputMode("append")
        .option("es.resource", index_name)
        .option("es.nodes", elastic_settings["url"])
        .option("es.port", "9243")
        .option("es.net.http.auth.user", elastic_settings["user"])
        .option("es.net.http.auth.pass", elastic_settings["password"])
        .option("es.nodes.wan.only", "true")
        .option("es.write.operation", "upsert")
        .option("es.mapping.id", document_id_column)
        .option("checkpointLocation", f"./checlpoints/{index_name}-checkpoint")
        # .start()
        # .outputMode("append").format("console").option("format", "json").start()
    )
    return query

# Define the schema for parsing Kafka messages
kafka_message_schema = StructType([
    StructField("movie", StructType([
        StructField("genres", ArrayType(StringType()), True),
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

# Define UDFs
get_rating_count_udf = udf(lambda x: find_movie_rating_count_by_id(x), IntegerType())
get_rating_avg_udf = udf(lambda x: find_movie_avg_rating_by_id(x), FloatType())
get_activity_count_udf = udf(lambda x: find_user_activity_count_by_id(x), IntegerType())


# Spark initialization
spark = initialize_spark()
# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "new_movies_reviews"
# Elasticsearch configuration
elastic_settings = {
    "url": os.getenv("ELASTIC_URL"),
    "api_key": os.getenv("ELASTIC_API_KEY"),
    "user": os.getenv("ELASTIC_USER"),
    "password": os.getenv("ELASTIC_PASSWORD")
}
# Read data from Kafka
kafka_data = read_from_kafka(spark, kafka_bootstrap_servers, kafka_topic)
# Parse JSON data from Kafka message
parsed_data = parse_kafka_message(kafka_data, kafka_message_schema)
# Parse JSON data from Kafka message for each schema
user_data_no_activity = parsed_data.select(col("user.*"))
movie_data_no_rating_avg = parsed_data.select(col("movie.*"), col("rating"))
review_data = parsed_data.select(concat_ws("_", col("user.userId"), col("movie.movieId")).alias("reviewId"),
                                          col("rating"), col("timestamp"), col("movie"), col("user"))

# Add old rating_count and rating_avg to movie_data
movie_data_old_rating = movie_data_no_rating_avg.withColumn("rating_count_old", get_rating_count_udf(movie_data_no_rating_avg["movieId"])) \
                                               .withColumn("rating_avg_old", get_rating_avg_udf(movie_data_no_rating_avg["movieId"]))
# Update movie_data with new rating_count and rating_avg
movie_data_with_rating = movie_data_old_rating.withColumn("rating_count", col("rating_count_old") + 1) \
                                             .withColumn("rating_avg", (col("rating_avg_old") * col("rating_count_old") + col("rating")) / (col("rating_count_old") + 1))
# Select only the columns we want
movie_data = movie_data_with_rating.select("movieId", "title", "genres", "rating_count", "rating_avg")

# Add old activity_count to user_data
user_data_old_activity = user_data_no_activity.withColumn("activity_count_old", get_activity_count_udf(user_data_no_activity["userId"]))
# Update user_data with new activity_count
user_data_with_activity = user_data_old_activity.withColumn("activity_count", col("activity_count_old") + 1)
# Select only the columns we want
user_data = user_data_with_activity.select("userId", "age", "gender", "occupation", "activity_count")

# # Change timestamp to date and time columns
# review_data = review_data_no_time.withColumn("timestamp_date", to_date(from_unixtime(col("timestamp")), "yyyy-MM-dd HH:mm:ss")).drop("timestamp")

# Mappings folder
mappings_folder = os.getenv("BASE_PROJECT_PATH") + "src/mappings/"
# Get files that end with .json
mapping_files = [f for f in os.listdir(mappings_folder) if f.endswith(".json")]
# Loop through the files
for mapping_file in mapping_files:
    # Get the index name
    index_name = mapping_file.split(".")[0]
    # Open the file and load the JSON
    with open(mappings_folder + mapping_file) as f:
        mapping = json.load(f)
    # Create the index in Elasticsearch
    create_index(index_name, mapping)

# Write data to Elasticsearch
query = write_to_elasticsearch(movie_data, "jayzz_movie_index", elastic_settings, "movieId").start()
query = write_to_elasticsearch(user_data, "jayzz_user_index", elastic_settings, "userId").start()
query = write_to_elasticsearch(review_data, "jayzz_review_index", elastic_settings, "reviewId").start()

# Wait for the termination signal
query.awaitTermination()