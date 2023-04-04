"""
Data Persistence Loader
Incremental Load

This script ingests the files in the temporal landing zone into the persistent zone in a MongoDB database.
Note that the files ingested with this script are those downloaded in the incremental collection in the temporal
landing zone.

"""

import os
from database_settings import mongo_utilities
from utilities import batch_ingest, headings_ingest

def load_exports():

    # Get all the paths of the files to upload
    folder = '../../data/temporal_landing/x/'
    files = [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]

    # Establish the connection to the database (persistent)
    db = mongo_utilities.connect()

    # Perform the ingestion
    batch_ingest(files, db, collection_name='peru_exports', loading_type='incremental')

def main():

    load_exports()


if __name__ == '__main__':
    main()