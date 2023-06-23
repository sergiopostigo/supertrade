"""
Data Persistent Loader
Utilities

"""

from simpledbf import Dbf5
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
from tqdm import tqdm
import re
from datetime import datetime
from database_settings import hdfs_utilities as hdfs
import numpy as np

def exports_ingestion(files_folder, log_context):

    """Ingest the exports DBF files in HDFS

       Ingest the files in DBF format into HDFS as Parquet files. Additionally, creates or updates a log to register
       the ingestion.

       Args:
           files_folder (str): path to the exports folder in the temporal landing zone
           log_context (string): context to add in the log file

       """

    # Get all the paths of the files to upload
    dbf_files = [os.path.join(files_folder, f) for f in os.listdir(files_folder) if os.path.isfile(os.path.join(files_folder, f)) if
                 f.endswith('.DBF')]

    if len(dbf_files)>0:
        # Create a parquet file for every DBF file:
        print('Converting {} DBF files to Parquet...'.format(len(dbf_files)))
        for file in tqdm(dbf_files):
            try:
                # Parse the DBF file into a dataframe
                batch = Dbf5(file, codec='latin-1')
                batch = batch.to_dataframe()
                # Add the batch week column
                batch['BATCH_WEEK'] = re.search(r'\d+', os.path.basename(file)).group()
                # Add the loading date column
                batch['LOAD_DATE'] = datetime.today().strftime('%Y%m%d')

                # Create the row groups
                # Get all the available boarding dates' years
                years = sorted(pd.to_datetime(batch['FEMB'], format='%Y%m%d').dt.year.unique().tolist(), reverse=True)
                # Convert the dataframe into a pyarrow table
                batch = pa.Table.from_pandas(batch)
                # For every year create a row group
                # In this case, we will include all the columns in the row group
                my_row_groups = []
                for year in years:
                    string_column = [str(i) for i in batch.column('FEMB').to_pylist()]
                    mask = [s.startswith(str(year)) for s in string_column]
                    filtered_table = batch.filter(mask)
                    # Get all the rows from that year
                    my_row_groups.append(filtered_table)
                # Create the Parquet file
                parquet_writer = pq.ParquetWriter(files_folder + os.path.basename(file).split('.')[0] + '.parquet',
                                                  my_row_groups[0].schema)
                # Add every row group
                for rg in my_row_groups:
                    parquet_writer.write_table(rg)
                parquet_writer.close()

            except Exception as e:
                print(f"Error generating parquet file for '{file}':{type(e).__name__}: {str(e)}")

            else:
                # Delete the DBF file from the temporal landing zone
                os.remove(os.path.abspath(os.path.join(files_folder, file)))
    else:
        print('No DBF files in the temporal landing zone')

    # Get all the parquet files paths
    parquet_files = [os.path.abspath(os.path.join(files_folder, file_name)) for file_name in os.listdir(files_folder) if
                     file_name.endswith('.parquet')]

    if len(parquet_files)>0:
        print('Ingesting {} parquet files into HDFS...'.format(len(parquet_files)))
        # Define the directory in HDFS to store the files
        hdfs_directory = '/thesis/peru/exports/'
        # Ingest the files in HDFS
        failed_count =[]
        for file in tqdm(parquet_files):
            result = hdfs.add_file_to_hdfs(file, hdfs_directory, log_context)
            # Delete the parquet file from the temporal landing zone if the transfer to HDFS was successfull
            if result == 0 :
                os.remove(file)
            failed_count.append(result)
        print('Ingestion finished! {} files ingested'.format(len(parquet_files)-np.sum(failed_count)))
    else:
        print('No parquet files in the temporal landing zone')

def headings_ingestion(file_path, log_context):

    """Ingest the headings file into HDFS

       Ingest the headings file in .txt format into HDFS as a Parquet file. Additionally, creates or updates a log to register
       the ingestion.

       Args:
           file_path (str): path to headings file in the temporal landing zone
           log_context (string): context to add in the log file

    """

    if os.path.exists(file_path):
        # Parse the file and convert it into a Dataframe
        with open(file_path, 'r') as f:
            file_lines = f.readlines()
            file_lines = [string.rstrip('\t\n') for string in file_lines][1:]
            file_lines = [string.split('\t') for string in file_lines]
            file_lines = [[element for element in inner_list if element.strip()] for inner_list in file_lines]
        # Convert to dataframe
        headings = pd.DataFrame(file_lines)
        # Convert the column names into strings
        headings.columns = headings.columns.astype(str)
        # Add the loading date column
        headings['LOAD_DATE'] = datetime.today().strftime('%Y%m%d')

        # Create a parquet file
        # Convert the dataframe into a pyarrow table
        headings = pa.Table.from_pandas(headings)
        # Generate the parquet file in the same folder than the original headings file
        parquet_writer = pq.ParquetWriter(os.path.dirname(file_path) + '/headings' + '.parquet', headings.schema)
        parquet_writer.write_table(headings)
        parquet_writer.close()


        # Define the directory in HDFS to store the files
        hdfs_directory = '/thesis/peru/headings/'
        # Add the files
        result = hdfs.add_file_to_hdfs(os.path.dirname(file_path) + '/headings.parquet', hdfs_directory,
                                       log_context=log_context)
        if result == 0:
            print('Ingestion finished! headings file added to HDFS')
            # Delete the parquet file
            os.remove(os.path.dirname(file_path) + '/headings.parquet')
            # Delete the original file
            os.remove(file_path)

        else:
            print('Ingestion of headings in HDFS failed!')
    else:
        print('No headings file in the temporal landing zone')