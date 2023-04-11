"""
PostgreSQL Database Setup

Create the Formatted Zone tables in PostgreSQL
"""

import postgres_utilities

def main():

    # Establish the connection with the database
    conn = postgres_utilities.connect()
    cursor = conn.cursor()

    create_tables(conn, cursor)

def create_tables(conn, cursor):

    try:
        # Create the table(s)
        cursor.execute(open("./formatted.sql", "r").read())
        conn.commit()
        print("Table(s) successfully created!")

    except Exception as e:
            print(f"Error while creating the table(s): {e}")

if __name__ == '__main__':
    main()