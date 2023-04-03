"""
Data Collector
Historical Collection

Download the data batches starting from a specific date till today

"""

import datetime
from utilities import data_collection

def main():
    start_date = datetime.date(2017, 1, 1)  # Set the start date
    data_collection(start_date, 'historical')  # Collect the data

if __name__ == '__main__':
    main()