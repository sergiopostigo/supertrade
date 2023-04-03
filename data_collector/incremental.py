"""
Data Collector
Incremental Collection

Download the data batches after the most recent batch in the persistent zone.

"""

import datetime
from utilities import data_collection
from database_settings import mongo_utilities

def main():

    # Get the most recent batch end date (+1 day)
    db = mongo_utilities.connect()
    collection = db['peru_exports']
    query = [
        {
            '$project': {
                'end_date': {'$substr': ['$BATCH_WEEK', 2, 2]},
                'end_month': {'$substr': ['$BATCH_WEEK', 4, 2]},
                'end_year': {'$substr': ['$BATCH_WEEK', 6, 2]}
            }
        },
        {'$sort': {'end_year': -1, 'end_month': -1, 'end_date': -1}},
        {'$limit': 1}]
    print("Getting the most recent batch end date...")
    result = collection.aggregate(query).next()
    start_date = (datetime.datetime.strptime(result['end_year'] + result['end_month'] + result['end_date'],
                                             '%y%m%d') + datetime.timedelta(days=1)).date() # add 1 day

    data_collection(start_date, 'incremental')  # Collect the data

if __name__ == '__main__':
    main()