import pandas as pd
import os
import subprocess
from datetime import datetime


def add_file_to_hdfs(file_path, hdfs_directory, log_context):

    """Add a file to HDFS

        Copy a file from the local filesystem into HDFS.

        Args:
            files_folder (str): Path to the exports folder in the temporal landing zone.
            log_context (str): Context to add in the log file.

        Returns:
            int: Returns 0 if the file was successfully stored in HDFS, and 1 otherwise.

    """

    # Construct the HDFS command
    insert_cmd = 'hadoop fs -put -f {} {}'.format(file_path, hdfs_directory)
    # Execute the HDFS command
    subprocess.run(insert_cmd, shell=True, capture_output=True, text=True)
    # Check if the file was properly uploaded
    try:
        # Execute the command and capture the output
        subprocess.check_output('hadoop fs -test -f {}{}'.format(hdfs_directory, os.path.basename(file_path)), shell=True)
    except subprocess.CalledProcessError:
        # If the command returns a non-zero exit code, the file does not exist
        print('Error uploading {}'.format(os.path.basename(file_path)))
        return 1
    else:
        # If the command returns a zero code, the file exists and do the following
        # Log to register the load
        logs = pd.DataFrame(columns=['filename', 'context', 'date', 'folder'])  # logs
        logs.loc[0] = [os.path.basename(file_path),
                       log_context,
                       datetime.today().date(),
                       hdfs_directory]
        logs.to_csv('./log.csv', mode='a', index=False,
                    header=not os.path.exists('./log.csv'))
        return 0