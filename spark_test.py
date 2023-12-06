from pyspark.sql import SparkSession

def test_spark():
    # Create a Spark session
    spark = SparkSession.builder.appName("SparkTest").getOrCreate()

    try:
        # Test Spark by creating a simple DataFrame
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        columns = ["Name", "Value"]
        df = spark.createDataFrame(data, columns)
        df.show()

    finally:
        # Stop the Spark session
        spark.stop()

test_spark()