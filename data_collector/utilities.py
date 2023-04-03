"""
Data Collector
Utilities

This script contains functions to download the specified batches of data from the following website into
the temporal landing zone:
http://www.aduanet.gob.pe/aduanas/informae/presentacion_bases_web.htm

Each batch corresponding to a specific week is defined by a code with a prefix and eight digits:

prefix: defines the type of data [x: exports, ma: imports (A), mb: imports (B), mam: imports (?)]
1st and 2nd digits: start day
3rd and 4th digits: end day
5th and 6th digits: month
7th and 8th digits: year

"""

import requests
import datetime
from tqdm import tqdm
from zipfile import ZipFile
import os
import pandas as pd

def week_code_generator(start_date, end_date):
    """Return the weekly data batch codes

    Get the weekly data batch codes for further search

    Args:
        start_date (datetime): Start date of batches
        end_date (datetime): End date of batches

    Returns:
        week_codes (list): List with the weekly codes
    """
    week_codes = []
    week_start = start_date - datetime.timedelta(days=start_date.weekday())
    week_end = week_start + datetime.timedelta(days=6)
    while week_start <= end_date:  # Loop until the end date is reached
        # Add the new code
        week_codes.append(
            week_start.strftime("%d") + week_end.strftime("%d") + week_end.strftime("%m") + week_end.strftime("%y"))
        # Move to the next week
        week_start = week_start + datetime.timedelta(days=7)
        week_end = week_start + datetime.timedelta(days=6)
    # Remove current week, since data is not uploaded yet
    week_codes.pop()
    #print("Week codes generated")
    return week_codes

def batch_downloader(prefixes, codes, temporal_landing_path, url, extension, collection_type):

    """Download batches of data

        Download and unzip the weekly batches of data according to the prefixes and codes defined.
        The data is stored in the temporal landing zone, inside folders according to the prefixes

        Args:
            prefixes (list): List with the prefixes to download
            codes (list): List with the date codes to download
            temporal_landing_path (string): Path of the temporal landing zone folder
            url (string): URL of the website to get the data from
            extension (string): Original extension of a download
            collection_type (string): historical or incremental download

        Returns:
    """
    if len(codes)>0:
        # Log to register the downloads and their status
        logs = pd.DataFrame(columns=['code', 'response', 'type', 'date'])  # logs

        # Download and unzip the files
        print("Downloading batches...")
        for prefix in prefixes:
            # Get the list of the files with that prefix, if any
            existing_files = [os.path.splitext(file)[0] for file in os.listdir(temporal_landing_path + prefix)]
            for code in tqdm(codes):
                # Proceed if the file isn't downloaded yet
                if prefix + code not in existing_files:
                    # Make the file request
                    response = requests.get(url + prefix + code + extension)
                    # Check if the response contains data
                    if response.status_code == 200:
                        # Save the response in a file
                        with open(temporal_landing_path + prefix + '/' + prefix + code + extension, "wb") as f:
                            f.write(response.content)
                        # Unzip the file and extract the content
                        with ZipFile(temporal_landing_path + prefix + '/' + prefix + code + extension, 'r') as f:
                            f.extractall(temporal_landing_path + prefix + '/')
                        # Delete the zipped file
                        os.remove(temporal_landing_path + prefix + '/' + prefix + code + extension)
                        # Register in the log
                        logs.loc[0] = [prefix + code, response.status_code, collection_type, datetime.date.today()]
                    else:
                        # Register the failure in the log
                        logs.loc[0] = [prefix + code, response.status_code, collection_type, datetime.date.today()]
                    # Save or update the log
                    logs.to_csv('./log.csv', mode='a', index=False,
                                header=not os.path.exists('./log.csv'))
        print("Download finished!")
    else:
        print("No new batches to download")

def data_collection(start_date, collection_type):

    """Collect the data

            Collect the data from a specific starting date till today

            Args:
                start_date (datetime): starting date
                collection_type (string): historical or incremental download

        """

    # Raise an error if the collection type is invalid
    valid_collection_types = {'incremental', 'historical'}
    if collection_type not in valid_collection_types:
        raise ValueError("collection type must be one of %r." % valid_collection_types)

    # Choose the start and end dates to collect the data
    start_date = start_date  # Set the start date
    end_date = datetime.date.today()  # Set the end date (today)

    # Further settings
    temporal_landing_path = '../../data/temporal_landing/'  # Set the temporal landing folder path
    url = 'http://www.aduanet.gob.pe/aduanas/informae/'  # Set the URL from which the data is collected
    prefixes = ['x']  # choose datasets: [x: exports, ma: imports (A), mb: imports (B), mam: imports (?)]
    extension = '.zip'  # extension of the data when it's downloaded

    # Get the codes
    codes = week_code_generator(start_date, end_date)
    # Download the data
    batch_downloader(prefixes, codes, temporal_landing_path, url, extension, collection_type)
