import psycopg2
from sqlalchemy import create_engine

def get_credentials():

    credentials = {
        'DB_NAME' : "formatted",
        'DB_USER' : "postgres",
        'DB_PASS' : "1234",
        'DB_HOST' : "localhost",
        'DB_PORT' : "5432"
    }
    return credentials

def connect():

    credentials = get_credentials()
    try:
        conn = psycopg2.connect(database=credentials['DB_NAME'],
                                user=credentials['DB_USER'],
                                password=credentials['DB_PASS'],
                                host=credentials['DB_HOST'],
                                port=credentials['DB_PORT'])
        print("Database connected successfully")
        return conn
    except:
        print("Database not connected successfully")

def engine():

    credentials = get_credentials()
    engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(
        credentials['DB_USER'],
        credentials['DB_PASS'],
        credentials['DB_HOST'],
        credentials['DB_NAME']
    ))
    return engine






