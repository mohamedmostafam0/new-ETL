import sys
import os
import psycopg2
from dotenv import load_dotenv  # Import dotenv

# Load environment variables from .env
load_dotenv()


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.constants import (
    POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST
)
print(f"Connecting to PostgreSQL at {POSTGRES_HOST} with user {POSTGRES_USER}")

def get_db_connection():
    """Establish a database connection and return the connection object."""
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=5432
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)  # Exit the script if the connection fails

def try_execute_sql(conn, sql: str):
    """Executes a given SQL statement using the provided database connection."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
            print("Executed table creation successfully")
    except Exception as e:
        print(f"Couldn't execute table creation due to exception: {e}")
        conn.rollback()

def create_table():
    """Creates the rappel_conso_table if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS rappel_conso_table (
        id SERIAL PRIMARY KEY,
        invoice_no TEXT,
        stock_code TEXT,
        description TEXT,
        quantity INTEGER,
        invoice_date TEXT,
        unit_price FLOAT,
        customer_id TEXT,
        country TEXT
    );
    """
    conn = get_db_connection()
    try_execute_sql(conn, create_table_sql)
    conn.close()

if __name__ == "__main__":
    create_table()
