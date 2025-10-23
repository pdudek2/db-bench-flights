import psycopg2
import os
from dotenv import load_dotenv
from common import load_csv, load_airlines, load_airports, load_flights
import argparse

load_dotenv()
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'database': os.getenv('POSTGRES_DATABASE')
}

POSTGRES_FLIGHTS_INSERT_SQL = """
    INSERT INTO flights (year, month, day_of_month, day_of_week, fl_date, op_unique_carrier, 
                         op_carrier_fl_num, origin, dest, crs_dep_time, crs_arr_time, crs_elapsed_time, distance)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING flight_id -- Required for get_postgres_last_id to work
"""

def get_postgres_last_id(cursor):
    """Retrieves the last inserted ID for PostgreSQL using fetchone."""
    # Assumes the INSERT query included 'RETURNING flight_id'
    result = cursor.fetchone()
    return result[0] if result else None

def load_data(file_name):
    """Main function to handle data loading process."""
    try:
        df = load_csv(file_name)
    except FileNotFoundError:
        print(f"ERROR: File not found: {file_name}")
        return
    except Exception as e:
        print(f"ERROR: Processing CSV file failed {e}")
        return

    conn = None  # Initialize conn
    try:
        # Changed: Connect using psycopg2
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Connected to PostgreSQL database")
    except psycopg2.Error as err:  # Changed: Catch psycopg2 error
        print(f"ERROR connecting to PostgreSQL database: {err}")
        return

    try:
        load_airlines(conn, cursor, df, insert_query = "INSERT INTO airline (carrier_code) VALUES (%s) ON CONFLICT (carrier_code) DO NOTHING")
        load_airports(conn, cursor, file_name,
                      insert_query="INSERT INTO airport (airport_code, city_name, state_name) VALUES (%s, %s, %s) ON CONFLICT (airport_code) DO NOTHING")
        load_flights(conn, cursor, df, POSTGRES_FLIGHTS_INSERT_SQL)

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"\nFATAL ERROR during data insertion: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    a = ap.parse_args()
    load_data(a.src)