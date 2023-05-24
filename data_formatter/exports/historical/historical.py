"""
Data Formatter
Exports Historical Formatting

Format and move the historical data of exports from the Persistent Zone to the Formatted Zone

"""

from database_settings import spark_utilities
from pyspark.sql.functions import col, lpad, concat_ws, regexp_replace, trim
from database_settings import postgres_utilities
from data_formatter import utilities

def main():

    # Get headings to work
    headings = utilities.get_headings()
    headings_filter = r"^(" + "|".join(headings) + ")"  # filter out headings that aren't in the list

    # Create a dataframe applying the following basic formatting:
    # - Merge the descriptions in a single column, replace NaN values by '' and trim white spaces
    # - Replace those exporter codes with value 'No Disponib' by unknown
    print("Performing query on Persistent Zone...")
    df = spark_utilities.get_spark_df('peru_exports') \
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
        .toPandas()


    # Add it to the formatted zone
    # Establish the connection with the database
    engine = postgres_utilities.engine()

    # Rename the columns and write in the database
    try:
        df.to_sql('peru_exports', engine, if_exists='append', index=False)
        print("Data sent to Formatted Zone successfully: {} rows added".format(len(df)))

    except Exception as e:
        print(f"Error while sending data to Formatted Zone: {e}")


if __name__ == '__main__':
    main()