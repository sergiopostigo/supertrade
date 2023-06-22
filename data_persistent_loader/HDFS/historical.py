"""
Data Persistence Loader
Historical Load

This script ingests the files in the temporal landing zone into the persistent zone in HDFS.
Note that the files ingested with this script are those downloaded in the historical collection in the temporal
landing zone.

"""

from project_settings import env
from utilities import exports_ingestion

def load_exports():

    # Get all the paths of the files to upload
    folder = env.TEMPORAL_LANDING_FOLDER+'x/'
    # Perform the ingestion
    exports_ingestion(files_folder=folder, log_context='historical')

def main():

    load_exports()

if __name__ == '__main__':
    main()