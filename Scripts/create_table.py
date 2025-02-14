import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2

from constants import (
    POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST
)

# Connect to the database
conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST)
cur = conn.cursor()


def try_execute_sql(sql: str):
    try:
        conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        print("Executed table creation successfully")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Couldn't execute table creation due to exception: {e}")
        conn.rollback()
        conn.close()

def create_table():
    """Creates the rappel_conso table."""
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
    try_execute_sql(create_table_sql)

if __name__ == "__main__":
    create_table()