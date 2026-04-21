import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")

def establish_db_connection():
    try:
        connection = psycopg2.connect(CONNECTION_STRING)
        print("Connection established successfully!")
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def create_tables(connection):
    try:
        cursor = connection.cursor()
        #Create ZipCodes table
        zip_table_query = """
        CREATE TABLE IF NOT EXISTS ZipCodes (
            zip_code VARCHAR(10) PRIMARY KEY,
            income INTEGER 
        )
        """
        cursor.execute(zip_table_query)
        connection.commit()

        #Create Parks table
        park_table_query = """
        CREATE TABLE IF NOT EXISTS Parks (
            park_id SERIAL PRIMARY KEY,
            park_name VARCHAR(255) NOT NULL,
            address VARCHAR(255) NOT NULL,
            zip_code VARCHAR(10) NOT NULL,
            FOREIGN KEY (zip_code) REFERENCES ZipCodes(zip_code)
        )
        """
        cursor.execute(park_table_query)
        connection.commit()

        # Create Events table
        event_table_query = """
        CREATE TABLE IF NOT EXISTS Events (
            event_id SERIAL PRIMARY KEY,
            movie_name VARCHAR(255) NOT NULL,
            park_id INTEGER NOT NULL, 
            rating VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            closed_captioning BOOLEAN,
            FOREIGN KEY (park_id) REFERENCES Parks(park_id)
        )
        """
        cursor.execute(event_table_query)
        connection.commit()

        print("Tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")




def main():
    conn = establish_db_connection()
    if conn:
        create_tables(conn)
        conn.close()

if __name__ == "__main__":
    main()