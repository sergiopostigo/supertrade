"""
Data Formatter
Exports Incremental Formatting

Format and move the new data of exports from the Persistent Zone to the Formatted Zone

"""

from database_settings import spark_utilities
from pyspark.sql.functions import col, lpad, concat_ws, regexp_replace, trim, to_date
from database_settings import postgres_utilities
from data_formatter import utilities
from database_settings.postgres_utilities import spark2postgres
from database_settings.spark_utilities import get_spark_df, spark_session

def main():

    # Get headings to work
    headings = utilities.get_headings()
    headings_filter = r"^(" + "|".join(headings) + ")"  # filter out headings that aren't in the list

    # Get the most recent batch week code from the data in the Formatted Zone
    # Establish the connection with the database
    conn = postgres_utilities.connect()
    cur = conn.cursor()
    # Query to get the most recent week range
    query = """
    SELECT to_date(SUBSTRING(BATCH_WEEK, 3, 8), 'DDMMYY') as LAST_DAY
    FROM peru_exports
    ORDER BY LAST_DAY DESC
    LIMIT 1;
    """
    # Execute the query
    cur.execute(query)
    last_day_of_data = cur.fetchone()[0]
    print('Most recent batch: {}'.format(last_day_of_data))

    # Spark session
    spark = spark_session()

    # Get new data from the Persistent Zone
    print("Performing query on Persistent Zone...")
    exports_path = '/thesis/peru/exports/*.parquet'  # exports path in HDFS
    df = get_spark_df(spark_session=spark, file_path=exports_path) \
        .select('PART_NANDI', 'VPESNET', 'VPESBRU', 'VFOBSERDOL', 'CPAIDES', 'NDOC', 'FEMB', 'DCOM', 'DMER2', 'DMER3',
                'DMER4', 'DMER5', 'BATCH_WEEK') \
        .withColumn("heading", lpad(col("PART_NANDI").cast("string"), 10, "0")) \
        .filter(col("heading").rlike(headings_filter)) \
        .filter(to_date(col("BATCH_WEEK").substr(-6, 6), 'ddMMyy') > last_day_of_data) \
        .withColumn("description", concat_ws(" ", col("DCOM"), col("DMER2"), col("DMER3"), col("DMER4"), col("DMER5"))) \
        .withColumn("description", regexp_replace(col("description"), "NaN", "")) \
        .withColumn("description", trim(col("description"))) \
        .withColumn("NDOC", regexp_replace(col("NDOC"), "No Disponib", "unknown")) \
        .withColumnRenamed('NDOC', 'exp_id') \
        .withColumnRenamed('VPESNET', 'net_weight') \
        .withColumnRenamed('VPESBRU', 'gross_weight') \
        .withColumnRenamed('VFOBSERDOL', 'value_usd') \
        .withColumnRenamed('CPAIDES', 'country') \
        .withColumnRenamed('FEMB', 'boarding_date') \
        .withColumnRenamed('BATCH_WEEK', 'batch_week') \
        .select('heading', 'exp_id', 'net_weight', 'gross_weight', 'value_usd', 'country', 'boarding_date',
                "description", 'batch_week')

    if df.count() > 0:
        # Write the DataFrame to the PostgreSQL database
        print("Sending data to PostgreSQL...")
        try:
            df.write \
                .format("jdbc") \
                .option("dbtable", "peru_exports") \
                .options(**spark2postgres()) \
                .mode('append') \
                .save()
            print("Data sent to Formatted Zone successfully: {} rows added".format(df.count()))

        except Exception as e:
            print(f"Error while sending data to Formatted Zone: {e}")
    else:
        print('Nothing to add to the Formatted Zone')


if __name__ == '__main__':
    main()