"""
Data Collector
Incremental Collection

Download the data batches after the most recent batch in the persistent zone.

"""

import datetime
from utilities import data_collection
from database_settings.spark_utilities import spark_session, get_spark_df
from pyspark.sql.functions import col, max, to_date

def main():

    print("Getting the most recent batch end date...")
    # Get the most recent batch end date (+1 day)
    # set the path of the exports parquet files in HDFS
    hdfs_path = '/thesis/peru/exports/*.parquet'
    # get the most recent date
    spark = spark_session()
    df = get_spark_df(spark_session=spark, file_path=hdfs_path)
    df = df.withColumn("date", to_date(col("BATCH_WEEK").substr(3, 6), "ddMMyy"))
    max_date = df.select(max("date")).first()[0]
    print("Most recent date:", max_date)

    start_date = max_date + datetime.timedelta(days=1) # add 1 day
    data_collection(start_date, 'incremental')  # Collect the data

if __name__ == '__main__':
    main()