"""
Data Persistence Loader
Utilities

"""

from database_settings import mongo_utilities, postgres_utilities
import os
from tqdm import tqdm
from simpledbf import Dbf5
from datetime import datetime
import pandas as pd
import re
from database_settings import mongo_utilities


def batch_ingest(dbf_paths, loading_type):

    """Ingest the batches files in MongoDB

       Ingest the files in DBF format into MongoDB as documents. Additionally, creates or updates a log to register
       the ingestion.

       Args:
           dbf_paths (list): path of the DBF files to ingest
           loading_type (string): historical or incremental (used for the log)

       """

    # Establish the connection to the database (persistent)
    database = mongo_utilities.connect()

    # Raise an error if the collection type is invalid
    valid_collection_types = {'incremental', 'historical'}
    if loading_type not in valid_collection_types:
        raise ValueError("loading type must be one of %r." % valid_collection_types)

    # Define the collection name
    collection_name = 'peru_exports'
    # Use (or create) the collection
    collection = database[collection_name]

    # Ingestion
    if len(dbf_paths)>0:

        print('Performing ingestion of batches...')
        r_count = 0
        ids_inserted = []
        for path in tqdm(dbf_paths):
            # Parse the DBF file into a dataframe
            batch = Dbf5(path, codec='latin-1')
            batch = batch.to_dataframe()
            # Add the batch week column
            batch['BATCH_WEEK'] = re.search(r'\d+', os.path.basename(path)).group()
            # Add the loading date column
            batch['LOAD_DATE'] = datetime.today().strftime('%Y%m%d')
            # Convert the dataframe into a dict
            batch = batch.to_dict('records')
            try:
                result = collection.insert_many(batch)
                # Update the count of documents ingested
                r_count = r_count + len(result.inserted_ids)
                # Update the list of documents ingested
                ids_inserted.extend(result.inserted_ids)
                # Status flag will be True if there are no errors during the ingestion
                status = True
            except Exception as e:
                print("An exception occurred ::", e)
                # Delete all documents ingested
                collection.delete_many({'_id': {'$in': ids_inserted}})
                # Status flag will be False if there is an error in the ingestion
                status = False
                break

        if status:

            print("{} ingestion finished!: {} documents".format(loading_type, r_count))

            # Log to register the ingestion
            logs = pd.DataFrame(columns=['n_rows', 'type', 'date', 'collection'])  # logs
            logs.loc[0] = [r_count, loading_type, datetime.today().date(), collection_name]
            logs.to_csv('./log.csv', mode='a', index=False,
                        header=not os.path.exists('./log.csv'))

            # Delete the files in the temporal landing zone
            for path in dbf_paths:
                os.remove(path)

    else:
        print('No batches to load')


def headings_ingest(file_path, loading_type):

    """Ingest the headings file (NANDINA.txt) in MongoDB

           Ingest the file in txt format into MongoDB. Additionally, creates or updates a log to register
           the ingestion.

           Args:
               file_path (string): path of the NANDINA.txt file to ingest
               loading_type (string): historical or incremental (used for the log)

           """
    # Establish the connection to the database (persistent)
    database = mongo_utilities.connect()

    # Raise an error if the collection type is invalid
    valid_collection_types = {'incremental', 'historical'}

    if loading_type not in valid_collection_types:
        raise ValueError("loading type must be one of %r." % valid_collection_types)

    # Define the collection name
    collection_name = 'peru_exports_headings'
    # Use (or create) the collection
    collection = database[collection_name]

    # Ingestion
    if len(file_path) > 0:

        print('Performing ingestion of headings...')

        # Get heading labels from the headings table obtained online
        with open(file_path, 'r') as f:
            file_lines = f.readlines()
            file_lines = [string.rstrip('\t\n') for string in file_lines][1:]
            file_lines = [string.split('\t') for string in file_lines]
            file_lines = [[element for element in inner_list if element.strip()] for inner_list in file_lines]
        # Convert to dataframe
        hs_labels = pd.DataFrame(file_lines)
        # Convert the column names into strings
        hs_labels.columns = hs_labels.columns.astype(str)
        # Add the loading date column
        hs_labels['LOAD_DATE'] = datetime.today().strftime('%Y%m%d')
        # Convert the dataframe into a dict
        hs_labels = hs_labels.to_dict('records')

        # Try ingestion
        try:
            result = collection.insert_many(hs_labels)
            # Get the count of headings ingested
            r_count = len(hs_labels)
            # Status flag will be True if there are no errors during the ingestion
            status = True
        except Exception as e:
            print("An exception occurred ::", e)
            # Status flag will be False if there is an error in the ingestion
            status = False

        if status:

            print("{} ingestion finished!: {} documents".format(loading_type, r_count))

            # Log to register the ingestion
            logs = pd.DataFrame(columns=['n_rows', 'type', 'date', 'collection'])  # logs
            logs.loc[0] = [r_count, loading_type, datetime.today().date(), collection_name]
            logs.to_csv('./log.csv', mode='a', index=False,
                        header=not os.path.exists('./log.csv'))

            # Delete the file in the temporal landing zone
            os.remove(file_path)