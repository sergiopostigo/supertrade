"""
Data Formatter
Headings Historical Formatting (Part 1)

Format the headings in the exports data from the Persistent Zone to perform a manual entity resolution of their descriptions

"""

from database_settings import spark_utilities
from pyspark.sql.functions import col, lpad
import pandas as pd
import time
from data_formatter import utilities

def main():

    # Get headings to work
    headings = utilities.get_headings()
    headings_filter = r"^(" + "|".join(headings) + ")"  # filter out headings that aren't in the list

    # Preprocessing:
    # Get all possible headings in the data in the persistent zone
    headings_to_format = spark_utilities.get_spark_df('peru_exports').select('PART_NANDI') \
        .distinct() \
        .withColumn("heading", lpad(col("PART_NANDI").cast("string"), 10, "0")) \
        .filter(col("heading").rlike(headings_filter)) \
        .select('heading') \
        .toPandas()

    # Get the labeled headings from the persistent zone
    headings_labeled = spark_utilities.get_spark_df('peru_exports_headings') \
        .select('0', '1') \
        .distinct() \
        .withColumnRenamed('1', 'raw_description') \
        .withColumn("heading", lpad(col("0").cast("string"), 10, "0")) \
        .select('heading', 'raw_description') \
        .toPandas()

    # Join both dataframes in the headings column
    my_headings = pd.merge(headings_to_format, headings_labeled, on='heading', how='left').sort_values(by='heading',
                                                                                                       ascending=True)
    # Cleaning
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
    my_headings['mapped_to'] = ''  # It may be the case that some headings in the list did not exist in the PDF, so the must be mapped to another headings that do exist.
    # Show count of rows to curate
    print('Rows to curate: ' + str(len(my_headings)))

    # Export to CSV for manual resolution of descriptions
    file_name = './hs_curated_' + str(int(time.time() * 1000)) + '.csv'
    my_headings.to_csv(file_name, index=None)
    print('Proceed with manual curation...')

if __name__ == '__main__':
    main()
