"""
Data Formatter
Headings Historical Formatting (Part 2)

Once manual curation is performed send the curated headings data to the Formatted Zone.

"""

import pandas as pd
from database_settings import postgres_utilities
from data_formatter import utilities
def main():

    # EDIT HERE (add the name of the file created in part 1)
    file_name = ''

    # Import the manually curated CSV
    curated_hs = pd.read_csv('./' + file_name, dtype={"heading": "string"})
    curated_hs['heading'] = curated_hs['heading'].apply(
        lambda x: x.zfill(10))  # during curation, zero padding may have been removed, so add it if needed

    # Add it to the formatted zone
    # Establish the connection with the database
    engine = postgres_utilities.engine()
    # Rename the columns and write in the database
    try:
        curated_hs[['heading', 'curated_description', 'mapped_to']] \
            .rename(columns={"curated_description": "description"}) \
            .to_sql('peru_exports_headings', engine, if_exists='append', index=False)
        print("Data sent to Formatted Zone successfully!")
        # Remove the manually curated file once it's data is already in the Formatted Zone
        utilities.delete_file_request(file_name)

    except Exception as e:
        print(f"Error while sending data to Formatted Zone: {e}")

if __name__ == '__main__':
    main()