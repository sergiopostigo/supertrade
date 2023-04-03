from pyspark.sql import SparkSession

def get_spark_df(collection):

    # Define the MongoDB configuration
    mongo_uri = "mongodb://localhost:27017/persistent.{}".format(collection)
    mongo_config = {"uri": mongo_uri}

    # Create the Spark session
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.driver.memory", "4g") \
        .config("spark.mongodb.read.connection.uri", mongo_uri) \
        .config("spark.mongodb.write.connection.uri", mongo_uri) \
        .getOrCreate()

    # Create a dataframe
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").options(**mongo_config).load()

    return df