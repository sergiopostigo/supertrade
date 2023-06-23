"""
Data Formatter
Exports Historical Formatting

Format and move the historical data of exports from the Persistent Zone to the Formatted Zone

"""

from database_settings.spark_utilities import get_spark_df, spark_session
from pyspark.sql.functions import col, lpad, concat_ws, regexp_replace, trim
from database_settings.postgres_utilities import spark2postgres
from data_formatter import utilities

def main():

    # Spark session
    spark = spark_session()

    # Get headings to work
    headings = utilities.get_headings()
    headings_filter = r"^(" + "|".join(headings) + ")"  # filter out headings that aren't in the list
    # Define the exports folder in HDFS
    path = '/thesis/peru/exports/*.parquet'

    # Formatting applied:
    # - Merge the descriptions in a single column, replace NaN values by '' and trim white spaces
    # - Replace those exporter codes with value 'No Disponib' by unknown
    print("Performing query on Persistent Zone...")
    #
    df = get_spark_df(spark_session=spark, file_path=path) \
        .select('PART_NANDI', 'VPESNET', 'VPESBRU', 'VFOBSERDOL', 'CPAIDES', 'NDOC', 'FEMB', 'DCOM', 'DMER2', 'DMER3',
                'DMER4', 'DMER5', 'BATCH_WEEK') \
        .withColumn("heading", lpad(col("PART_NANDI").cast("string"), 10, "0")) \
        .filter(col("heading").rlike(headings_filter)) \
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
                "description", 'batch_week') \
        .orderBy(col('heading').asc())

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


if __name__ == '__main__':
    main()