import findspark
findspark.init()
from flask import Flask, jsonify, request
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from elasticsearch import Elasticsearch

load_dotenv()

app = Flask(__name__)

# Load Spark session
spark = SparkSession.builder.appName("MovieRecommendation").getOrCreate()

# Load the ALS model
model_path = os.getenv('BASE_PROJECT_PATH') + 'recommendation/best_model_als'
als_model = ALSModel.load(model_path)

# function to find movie by title
def find_movieId_by_title(title: str):
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
def get_most_active_users_that_rated_movie(movie_id: int):
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
                        "movie.movieId": movie_id
                    }
                }
            }
        )
        # close the Elasticsearch connection
        elastic_client.close()

        return [int(hit["_source"]["user"]["userId"]) for hit in response["hits"]["hits"]]
    except Exception as e:
        return [e]
    
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
        return None

def get_top_recommendations_for_user(user_id, num_recommendations=10):
    user_df = spark.createDataFrame([(user_id,)], ["userId"])
    recommendations = als_model.recommendForUserSubset(user_df, num_recommendations)
    movie_ids = [row for row in recommendations.collect()[0]["recommendations"]]
    return movie_ids

def get_top_recommendations_for_users(user_ids, num_recommendations=3):
    user_df = spark.createDataFrame([(user_id,) for user_id in user_ids], ["userId"])
    recommendations = als_model.recommendForUserSubset(user_df, num_recommendations)
    user_recommendations = [
        {
            "userId": row["userId"],
            "movieIds": [movie_id for movie_id in row["recommendations"]]
        }
        for row in recommendations.collect()
    ]
    return user_recommendations

# function to know if a movie is already in the database
def is_movie_in_database(movie_id: int):
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

        # extract the movieId
        movie = response["hits"]["hits"][0]["_source"]

        # return the movieId
        return movie
    except Exception as e:
        return None

# recommand movies for a given movie title
@app.route("/recommendation/movie", methods=["GET"])
def get_recommendations_for_movie():
    title = request.args.get("title")
    movie_id = find_movieId_by_title(title)
    user_ids = get_most_active_users_that_rated_movie(movie_id)
    user_recommendations = get_top_recommendations_for_users(user_ids)
    # movie_ids = [movie_id for user_recommendation in user_recommendations for movie_id in user_recommendation["movieIds"]]
    # print(movie_ids)
    # movies = find_movies_by_movie_ids(movie_ids)
    return jsonify(user_recommendations)

# change the default port to 5001
if __name__ == "__main__":
    app.run(port=5001, debug=True)