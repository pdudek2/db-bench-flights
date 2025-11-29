import mysql.connector
import os
from .common import load_csv, load_airlines, load_airports, load_flights
import argparse

DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'mysql'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': 'root',
    'password': os.getenv('MYSQL_ROOT_PASSWORD', 'super_password'),
    'database': os.getenv('MYSQL_DATABASE', 'flights')
}

def get_mysql_connection():
    return mysql.connector.connect(**DB_CONFIG)

MYSQL_FLIGHTS_INSERT_SQL = """
    INSERT INTO flights (year, month, day_of_month, day_of_week, fl_date, op_unique_carrier, 
                         op_carrier_fl_num, origin, dest, crs_dep_time, crs_arr_time, crs_elapsed_time, distance)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def get_mysql_last_id(cursor):
    return cursor.lastrowid

def import_to_mysql(file_name):
    try:
        df = load_csv(file_name)
    except FileNotFoundError:
        print(f"ERROR: File not found: {file_name}")
        return
    except Exception as e:
        print(f"ERROR: Processing CSV file failed {e}")
        return

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Connected to MySQL database")
    except mysql.connector.Error as err:
        print(f"ERROR connectiong to MYSQL database: {err}")
        return

    try:
        load_airlines(conn, cursor, df, insert_query="INSERT IGNORE INTO airline (carrier_code) VALUES (%s)")
        load_airports(conn, cursor, file_name,
                      insert_query="INSERT IGNORE INTO airport (airport_code, city_name, state_name) VALUES (%s, %s, %s)")
        load_flights(conn, cursor, df, MYSQL_FLIGHTS_INSERT_SQL, get_mysql_last_id)
    except Exception as e:
        conn.rollback()
        print(f"\nERROR when inserting data. Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    a = ap.parse_args()
    import_to_mysql(a.src)
