"""
Data Persistence Loader
Historical Load

This script ingests the files in the temporal landing zone into the persistent zone in HDFS.
Note that the files ingested with this script are those downloaded in the historical collection in the temporal
landing zone.

"""

from project_settings import env
from utilities import exports_ingestion, headings_ingestion

def load_exports():

    print('Exports historical load into HDFS...')
    # Get all the paths of the files to upload
    folder = env.TEMPORAL_LANDING_FOLDER+'x/'
    # Perform the ingestion
    exports_ingestion(files_folder=folder, log_context='historical')

def load_headings():

    print('Headings historical load into HDFS...')
    # Get the path of the headings file
    file = env.TEMPORAL_LANDING_FOLDER + '/headings/NANDINA.txt'
    # Perform the ingestion
    headings_ingestion(file_path=file, log_context='historical')

def main():

    print('--DATA PERSISTENT ZONE HISTORICAL LOAD--')
    load_exports()
    load_headings()

if __name__ == '__main__':
    main()