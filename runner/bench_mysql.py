import os
import time
from mysql.connector.pooling import MySQLConnectionPool

from bench_common import log_result

_POOL = None

def mysql_conn():
    global _POOL
    if _POOL is None:
        _POOL = MySQLConnectionPool(
            pool_name="bench_pool",
            pool_size=int(os.getenv("MYSQL_POOL_SIZE", 5)),
            host=os.getenv('MYSQL_HOST', "localhost"),
            port=int(os.getenv('MYSQL_PORT', 3306)),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE'),
            autocommit=False
        )
    return _POOL.get_connection()

def warmup_mysql():
    conn = mysql_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT flight_id from flights LIMIT 1")
        cur.fetchone()
        conn.commit()
        return None
    finally:
        cur.close()
        conn.close()

def s_mysql_add_flight(cfg, iteration: int):
    flight = cfg["queries"]["insert_flight"]["flights"][iteration - 1]
    cols = [
        "year", "month", "day_of_month", "day_of_week", "fl_date",
        "op_unique_carrier", "op_carrier_fl_num", "origin", "dest",
        "crs_dep_time", "crs_arr_time", "crs_elapsed_time", "distance"
    ]
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f"INSERT INTO flights ({', '.join(cols)}) VALUES ({placeholders})"
    vals = [
            int(flight["year"]),
            int(flight["month"]),
            int(flight["day_of_month"]),
            int(flight["day_of_week"]),
            flight["fl_date"],
            flight["op_unique_carrier"],
            str(flight["op_carrier_fl_num"]),
            flight["origin"],
            flight["dest"],
            int(flight.get("crs_dep_time", 0)),
            int(flight.get("crs_arr_time", 0)),
            int(flight.get("crs_elapsed_time", 0)),
            int(flight.get("distance", 0))
    ]
    t0 = time.perf_counter()
    conn = mysql_conn()
    cur = conn.cursor()
    try:
        cur.execute(insert_sql, vals)
        conn.commit()
        inserted_id = cur.lastrowid
        cfg.setdefault("queries", {}).setdefault("update_flight", {}).setdefault("_inserted_ids", []).append(inserted_id)
        dt = (time.perf_counter() - t0) * 1000
        return dt, f"inserted_id={inserted_id}"
    finally:
        cur.close()
        conn.close()

def s_mysql_add_flight_stats(cfg, iteration: int):
    flight_id = cfg["queries"]["update_flight"]["_inserted_ids"][iteration - 1]
    perf = cfg["queries"]["update_flight"]["flight_performance"][iteration - 1]
    delayed_list = cfg["queries"]["update_flight"].get("flights_delayed", [])
    delayed_entry = next((d for d in delayed_list if d.get("flight_index") == (iteration - 1)), None)

    t0 = time.perf_counter()
    conn = mysql_conn(); cur = conn.cursor()
    try:
        inserted_delayed = 0
        if delayed_entry:
            cur.execute(
                "INSERT INTO flights_delayed (flight_id, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    int(flight_id),
                    int(delayed_entry.get("carrier_delay", 0)),
                    int(delayed_entry.get("weather_delay", 0)),
                    int(delayed_entry.get("nas_delay", 0)),
                    int(delayed_entry.get("security_delay", 0)),
                    int(delayed_entry.get("late_aircraft_delay", 0)),
                )
            )
            inserted_delayed = cur.rowcount or 1
            cur.execute(
                "INSERT INTO flights_performance (flight_id, dep_time, dep_delay, taxi_out, wheels_off, wheels_on, taxi_in, "
                "arr_time, arr_delay, actual_elapsed_time, air_time, diverted, delay_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    int(flight_id),
                    int(perf.get("dep_time", 0)),
                    int(perf.get("dep_delay", 0)),
                    int(perf.get("taxi_out", 0)),
                    int(perf.get("wheels_off", 0)),
                    int(perf.get("wheels_on", 0)),
                    int(perf.get("taxi_in", 0)),
                    int(perf.get("arr_time", 0)),
                    int(perf.get("arr_delay", 0)),
                    int(perf.get("actual_elapsed_time", 0)),
                    int(perf.get("air_time", 0)),
                    bool(perf.get("diverted", False)),
                    int(flight_id),  # delay_id -> flights_delayed.flight_id
                )
            )
        else:
            cur.execute(
                "INSERT INTO flights_performance (flight_id, dep_time, dep_delay, taxi_out, wheels_off, wheels_on, taxi_in, "
                "arr_time, arr_delay, actual_elapsed_time, air_time, diverted, delay_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    int(flight_id),
                    int(perf.get("dep_time", 0)),
                    int(perf.get("dep_delay", 0)),
                    int(perf.get("taxi_out", 0)),
                    int(perf.get("wheels_off", 0)),
                    int(perf.get("wheels_on", 0)),
                    int(perf.get("taxi_in", 0)),
                    int(perf.get("arr_time", 0)),
                    int(perf.get("arr_delay", 0)),
                    int(perf.get("actual_elapsed_time", 0)),
                    int(perf.get("air_time", 0)),
                    bool(perf.get("diverted", False)),
                    None
                )
            )
        conn.commit()
        dt = (time.perf_counter() - t0) * 1000
        note = f"flight_id={flight_id}, perf_inserted=1"
        if delayed_entry:
            note += f", delayed_inserted={inserted_delayed}"
        return dt, note
    finally:
        cur.close(); conn.close()

def s_mysql_top_routes_month(cfg, iteration: int):
    raise NotImplemented

def s_mysql_histogram_arr_delay(cfg, iteration: int):
    raise NotImplemented

def s_mysql_find_flights_route_range_with_stats(cfg, iteration: int):
    raise NotImplemented

def s_mysql_get_flight_with_stats(cfg, iteration: int):
    raise NotImplemented

def s_mysql_rank_punctual_airlines(cfg, iteration: int):
    raise NotImplemented

SCENARIOS_MYSQL = [
    ("mysql_add_flight", s_mysql_add_flight),
    ("mysql_add_flight_stats", s_mysql_add_flight_stats)
    # ("mysql_top_routes_month", s_mysql_top_routes_month),
    # ("mysql_histogram_arr_delay", s_mysql_histogram_arr_delay),
    # ("mysql_find_route_with_stats", s_mysql_find_flights_route_range_with_stats),
    # ("mysql_get_flight_with_stats", s_mysql_get_flight_with_stats),
    # ("mysql_rank_punctual_airlines", s_mysql_rank_punctual_airlines),
]

def run_mysql(cfg):
    warmup_mysql()
    for name, fn in SCENARIOS_MYSQL:
        for r in range(1, int(cfg["repeats"]) + 1):
            dt, notes = fn(cfg, r)
            log_result("mysql", cfg["dataset_name"], name, r, dt, notes)
            print(f"[mysql][{name}][run={r}] {dt:.2f} ms :: {notes}")