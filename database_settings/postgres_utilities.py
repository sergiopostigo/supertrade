import psycopg2
from sqlalchemy import create_engine, text

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
        print("Connected to Formatted Zone successfully!")
        return conn
    except:
        print("Connection to Formatted Zone failed")

def engine(message_flag=True):

    credentials = get_credentials()
    try:
        engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(
            credentials['DB_USER'],
            credentials['DB_PASS'],
            credentials['DB_HOST'],
            credentials['DB_NAME']
        ))
        if message_flag:
            print("Connected to Formatted Zone successfully!")

        return engine
    except:
        print("Connection to Formatted Zone failed")


def run_query_file(engine, path, params=None):

    with open(path, 'r') as f:
        query = f.read()
        if params:
            query = text(query).bindparams(**params)

    try:
        results = engine.execute(query)
        if results.returns_rows:
            return results.fetchall()
        else:
            print('Query run successfully!')

    except Exception as e:
        print(f"An error occurred while executing the query: {e}")





