"""
Data Formatter
Headings Incremental Formatting (Part 1)

Formatting unseen headings (new headings in the persistent zone that do not exist already in the formatted zone).
In this first part, generate the manual curation file for unseen headings.

"""

from database_settings import spark_utilities
from pyspark.sql.functions import col, lpad,concat_ws
import pandas as pd
import time
from database_settings import postgres_utilities
from data_formatter import utilities
from database_settings.spark_utilities import get_spark_df, spark_session


def main():

    # Get headings to work
    headings = utilities.get_headings()
    headings_filter = r"^(" + "|".join(headings) + ")"  # filter out headings that aren't in the list

    # Get the headings that already exist in the Formatted Zone
    # Establish the connection with the Formatted Zone
    engine = postgres_utilities.engine()
    # Read the headings table
    existing_headings = pd.read_sql_table('peru_exports_headings', engine)

    # Spark session
    spark = spark_session()

    # Get the headings in the Persistent Zone that do not exist in the Formatted Zone (those starting with the digits in the headings list defined above)
    exports_path = '/thesis/peru/exports/*.parquet'  # Files in the Persistent Zone
    headings_to_format = get_spark_df(spark_session=spark, file_path=exports_path) \
        .select('PART_NANDI') \
        .distinct() \
        .withColumn("heading", lpad(col("PART_NANDI").cast("string"), 10, "0")) \
        .filter(col("heading").rlike(headings_filter)) \
        .filter(~(col("heading").isin(existing_headings['heading'].to_list()))) \
        .select('heading') \
        .toPandas()

    # Get the labeled headings from the persistent zone
    headings_path = '/thesis/peru/headings/headings.parquet'  # Files in the Persistent Zone
    headings_labeled = get_spark_df(spark_session=spark, file_path=headings_path) \
        .select('0', '1') \
        .distinct() \
        .withColumnRenamed('1', 'raw_description') \
        .withColumn("heading", lpad(col("0").cast("string"), 10, "0")) \
        .select('heading', 'raw_description') \
        .toPandas()

    # Join both dataframes in the headings column
    my_headings = pd.merge(headings_to_format, headings_labeled, on='heading', how='left').sort_values(by='heading',
                                                                                                       ascending=True)

    # Formatting
    # Eliminate multiple spacings in the description
    my_headings['raw_description'].replace(r'\s+', ' ', regex=True, inplace=True)
    # Eliminate the dashes at the beginning of the description
    my_headings['raw_description'].replace(r'^[-\s]+', '', regex=True, inplace=True)
    # Add final dot to description
    my_headings['raw_description'] = my_headings['raw_description'].apply(lambda x: x if x.endswith('.') else x + '.')
    # Group all descriptions from a heading in a single cell
    my_headings = my_headings.groupby('heading')['raw_description'].agg(lambda x: ' '.join(x)).reset_index()
    # Rename the description
    my_headings.rename(columns={'raw_description': 'merged_description'}, inplace=True)
    # Add additional columns for curated description and mappings
    my_headings['curated_description'] = ''
    my_headings[
        'mapped_to'] = ''  # It may be the case that some headings in the list did not exist in the PDF, so the must be mapped to another headings that do exist.
    # Show count of rows to curate
    print('Rows to curate: ' + str(len(my_headings)))

    # Export to CSV for manual resolution of descriptions
    if (len(my_headings) > 0):
        file_name = './hs_curated_' + str(int(time.time() * 1000)) + '.csv'
        my_headings.to_csv(file_name, index=None)
        print("Proceed to manual curation process")
    else:
        print("No new headings to curate")

if __name__ == '__main__':
    main()