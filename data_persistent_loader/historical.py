"""
Data Persistence Loader
Historical Load

This script ingests the files in the temporal landing zone into the persistent zone in a MongoDB database.
Note that the files ingested with this script are those downloaded in the historical collection in the temporal
landing zone.

"""

import os
from database_settings import mongo_utilities
from utilities import batch_ingest, headings_ingest

def load_exports():

    # Get all the paths of the files to upload
    folder = '../../data/temporal_landing/x/'
    files = [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]

    # Perform the ingestion
    batch_ingest(files, loading_type='historical')

def load_headings():

    # Get the path of the headings file
    file = '../../data/temporal_landing/support/NANDINA.TXT'

    # Perform the ingestion
    headings_ingest(file, loading_type='historical')

def main():

    load_exports()
    load_headings()

if __name__ == '__main__':
    main()