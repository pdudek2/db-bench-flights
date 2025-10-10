import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv
import argparse

load_dotenv()
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE')
}

def load_data(file_name):
    print(f"File: {file_name}")
    try:
        df = pd.read_csv(file_name, na_values=["", " "])
        df = df.drop(columns=['origin_city_name', 'origin_state_nm',
                              'dest_city_name', 'dest_state_nm'])

        for col in ['flight_id', 'dep_time', 'arr_time', 'crs_dep_time', 'crs_arr_time', 'air_time', 'actual_elapsed_time', 'arr_delay']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)

        df = df.replace({float('NaN'): None})
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
        airlines = set()
        airports = {}

        print("\n--- Loading (AIRLINE, AIRPORT) tables ---")

        for carrier in df['op_unique_carrier'].unique():
            if carrier: airlines.add(carrier)
        for carrier in airlines:
            try:
                cursor.execute(
                    "INSERT IGNORE INTO airline (carrier_code) VALUES (%s)", (carrier,)
                )
            except Exception as e:
                print(f"Error when inserting to airline ({carrier}): {e}")

        airport_data = pd.read_csv(CSV_FILE)[['origin', 'origin_city_name', 'origin_state_nm',
                                              'dest', 'dest_city_name', 'dest_state_nm']].drop_duplicates()

        for _, row in airport_data.iterrows():
            if row['origin'] not in airports and pd.notna(row['origin']):
                airports[row['origin']] = (row['origin_city_name'], row['origin_state_nm'])
            if row['dest'] not in airports and pd.notna(row['dest']):
                airports[row['dest']] = (row['dest_city_name'], row['dest_state_nm'])

        for code, (city, state) in airports.items():
            try:
                cursor.execute(
                    "INSERT IGNORE INTO airport (airport_code, city_name, state_name) VALUES (%s, %s, %s)",
                    (code, city, state)
                )
            except Exception as e:
                print(f"ERROR when inserting to airport ({code}): {e}")

        conn.commit()
        print("Tables 'airline; and 'airports' succesfully loaded.")

        print("\n--- Loading flights ---")

        for index, row in df.iterrows():
            flights_sql = """
                INSERT INTO flights (year, month, day_of_month, day_of_week, fl_date, op_unique_carrier, 
                                     op_carrier_fl_num, origin, dest, crs_dep_time, crs_arr_time, crs_elapsed_time, distance)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            fl_date_obj = row['fl_date'].split()[0] if isinstance(row['fl_date'], str) else row['fl_date']
            flights_values = (
                row['year'], row['month'], row['day_of_month'], row['day_of_week'], fl_date_obj,
                row['op_unique_carrier'], row['op_carrier_fl_num'], row['origin'], row['dest'],
                row['crs_dep_time'], row['crs_arr_time'], row['crs_elapsed_time'], row['distance']
            )

            cursor.execute(flights_sql, flights_values)
            flight_id = cursor.lastrowid

            is_cancelled = row['cancelled'] == 1
            performance_id = None
            cancellation_id = None

            if is_cancelled:
                cancelled_sql = "INSERT INTO flights_cancelled (flight_id, cancellation_code) VALUES (%s, %s)"
                cursor.execute(cancelled_sql, (flight_id, row['cancellation_code']))
                cancellation_id = flight_id
            else:
                performance_sql = """
                    INSERT INTO flights_performance (flight_id, dep_time, dep_delay, taxi_out, wheels_off, wheels_on, taxi_in, 
                                                     arr_time, arr_delay, actual_elapsed_time, air_time, diverted)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                performance_values = (
                    flight_id, row['dep_time'], row['dep_delay'], row['taxi_out'], row['wheels_off'], row['wheels_on'],
                    row['taxi_in'], row['arr_time'], row['arr_delay'], row['actual_elapsed_time'], row['air_time'],
                    bool(row['diverted'] == 1)
                )
                cursor.execute(performance_sql, performance_values)
                performance_id = flight_id
                total_delay_reasons = (row['carrier_delay'] or 0) + (row['weather_delay'] or 0) + \
                                      (row['nas_delay'] or 0) + (row['security_delay'] or 0) + \
                                      (row['late_aircraft_delay'] or 0)

                if total_delay_reasons > 0:
                    delayed_sql = """
                        INSERT INTO flights_delayed (flight_id, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    delayed_values = (
                        flight_id, row['carrier_delay'], row['weather_delay'], row['nas_delay'],
                        row['security_delay'], row['late_aircraft_delay']
                    )
                    cursor.execute(delayed_sql, delayed_values)
                    cursor.execute(
                        "UPDATE flights_performance SET delay_id = %s WHERE flight_id = %s",
                        (flight_id, flight_id)
                    )

            status_sql = "INSERT INTO flight_status (flight_id, performance_id, cancellation_id) VALUES (%s, %s, %s)"
            cursor.execute(status_sql, (flight_id, performance_id, cancellation_id))

            if (index + 1) % 1000 == 0:
                conn.commit()
                print(f"Loaded {index + 1} flights...")
        conn.commit()
        print(f"\nLoading flights has finished.")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR when inserting data. Error: {e} for record number: {index}")
        print(f"ERROR Row value: {row}")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    a = ap.parse_args()
    load_data(a.src)