from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaToElastic").getOrCreate()

# Initialize Elasticsearch client
elastic_client = Elasticsearch(
    os.getenv("ELASTIC_URL"),
    api_key=(os.getenv("ELASTIC_API_KEY"))
)

# Mappings
movie_index_mapping = {
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "movieId": {
        "type": "keyword"
      },
      "genres": {
        "type": "keyword"
      },
      "title": {
        "type": "text"
      }
    }
  }
}

review_index_mapping = {
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "movie": {
        "properties": {
          "genres": {
            "type": "keyword"
          },
          "movieId": {
            "type": "keyword"
          },
          "title": {
            "type": "text"
          }
        }
      },
      "rating": {
        "type": "float"
      },
      "timestamp": {
        "type": "date"
      },
      "user": {
        "properties": {
          "age": {
            "type": "integer"
          },
          "gender": {
            "type": "keyword"
          },
          "occupation": {
            "type": "keyword"
          },
          "userId": {
            "type": "keyword"
          }
        }
      }
    }
  }
}

user_index_mapping = {
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "age": {
        "type": "integer"
      },
      "gender": {
        "type": "keyword"
      },
      "occupation": {
        "type": "keyword"
      },
      "userId": {
        "type": "keyword"
      }
    }
  }
}

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaToElastic") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("local[*]") \
        .getOrCreate()

def read_from_kafka_topic(topic_name):
    kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

    return kafka_df


def transform_and_clean_data(raw_data):
    # Define the schema for the raw data
    schema = StructType([
        StructField("page", IntegerType()),
        StructField("results", ArrayType(
            StructType([
                StructField("movie", StructType([
                    StructField("genres", ArrayType(StringType())),
                    StructField("movieId", StringType()),
                    StructField("title", StringType())
                ])),
                StructField("rating", StringType()),
                StructField("timestamp", StringType()),
                StructField("user", StructType([
                    StructField("age", StringType()),
                    StructField("gender", StringType()),
                    StructField("occupation", StringType()),
                    StructField("userId", StringType())
                ]))
            ])
        )),
        StructField("total_pages", IntegerType()),
    ])

    # Apply schema to the raw data and explode the "results" array
    cleaned_data = raw_data \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .selectExpr("page", "explode(results) as result") \
        .select("page", "result.*")

    # Perform additional transformations if needed

    return cleaned_data


def write_to_elasticsearch(data, index_name, mapping, client):
    data.writeStream \
        .outputMode("append") \
        .foreach(lambda row: client.index(
            index=index_name,
            body=row.asDict(),
            doc_type='_doc'  # Specify the document type if using Elasticsearch 6.x
        )) \
        .start()

# Example Usage
kafka_topic = "reviews"

# Read data from Kafka topic
raw_kafka_data = read_from_kafka_topic(kafka_topic)

# Print the raw data to console
raw_kafka_data.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

# # Transform and clean the data
# cleaned_data = transform_and_clean_data(raw_kafka_data)

# # Write the data to Elasticsearch indexes
# write_to_elasticsearch(cleaned_data.filter(col("movie").isNotNull()), "movies", movie_index_mapping, elastic_client)
# write_to_elasticsearch(cleaned_data.filter(col("movie").isNotNull()), "reviews", review_index_mapping, elastic_client)
# write_to_elasticsearch(cleaned_data.filter(col("user").isNotNull()), "users", user_index_mapping, elastic_client)

# # Start Spark streaming
# spark.streams.awaitAnyTermination()
