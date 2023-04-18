from pymongo import MongoClient

def get_credentials():

    credentials = {
        'DB_NAME': "persistent",
        'DB_HOST': "localhost",
        'DB_PORT': 27017
    }
    return credentials

def connect():

    credentials = get_credentials()
    try:
        client = MongoClient(credentials['DB_HOST'], credentials['DB_PORT'])
        db = client[credentials['DB_NAME']]
        print("Connected to Persistent Zone successfully!")
        return db # Returns the db cursor
    except Exception as e:
        print("An exception occurred ::", e)
