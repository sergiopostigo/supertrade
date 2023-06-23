from pyspark.sql import SparkSession


def spark_session():

    # Define the HDFS configuration
    hdfs_uri = "hdfs://localhost:9000/"

    # Create the Spark session
    spark = SparkSession \
        .builder \
        .appName("hdfs_query") \
        .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark

def get_spark_df(spark_session, query=None, file_path=None):

    # Ensure that whether query or file_path is provided
    if query is None and file_path is None:
        raise ValueError("Either 'query' or 'file_path' must be provided.")
    if query is not None and file_path is not None:
        raise ValueError("Only one of 'query' or 'file_path' should be provided.")

    # If a query was provided, return a df with its result
    if query:
        df = spark_session.sql(query.format(query))

    # If a file_path was provided, return a df with the content of that file
    elif file_path:
        df = spark_session.read.parquet(file_path)

    return df