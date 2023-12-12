# Import packages
import os
from dotenv import load_dotenv
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

# function to find movie by title
def find_movie_by_title(title: str):
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
                        "title": title
                    }
                }
            }
        )
        # close the Elasticsearch connection
        elastic_client.close()

        # extract the movieId
        movie_id = int(response["hits"]["hits"][0]["_source"]["movieId"])

        # return the movieId
        return movie_id
    except Exception as e:
        return None

# function to find users who rated a movieId
def find_users_by_movie_id(movie_id: int):
    try:
        elastic_client = Elasticsearch(
            os.getenv("ELASTIC_URL"),
            api_key=(os.getenv("ELASTIC_API_KEY"))
        )
        # Use Elasticsearch.search method
        response = elastic_client.search(
            index="jayzz_review_index",
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

        # return a list of userIds
        users_ids =  [int(hit["_source"]["userId"]) for hit in response["hits"]["hits"]]

        # unique users
        return list(set(users_ids))
        # unique users
        return list(set(users_ids))
    except Exception as e:
        return []
    
# function to find movies by list of movieIds
def find_movies_by_movie_ids(movie_ids: list):
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
                    "terms": {
                        "movieId": movie_ids
                    }
                }
            }
        )

        # close the Elasticsearch connection
        elastic_client.close()

        # return a list of movie objects
        return [hit["_source"] for hit in response["hits"]["hits"]]
    except Exception as e:
        return []

# export the functions
__all__ = [
    "create_index",
    "find_movie_rating_count_by_id",
    "find_movie_avg_rating_by_id",
    "find_user_activity_count_by_id",
    "find_movie_by_title",
    "find_users_by_movie_id",
    "find_movies_by_movie_ids"
]