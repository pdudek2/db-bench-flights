import os
import time
from psycopg2 import pool
from bench_common import log_result

_POOL = None

def postgres_conn():
    global _POOL
    if _POOL is None:
        _POOL = pool.ThreadedConnectionPool(
            1,
            int(os.getenv("PG_POOL_SIZE", 5)),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            user=os.getenv("POSTGRES_USER", "bench"),
            password=os.getenv("POSTGRES_PASSWORD", "bench"),
            dbname=os.getenv("POSTGRES_DB", "flights_db")
        )
    return _POOL.getconn()

def _put_conn(conn):
    global _POOL
    if _POOL:
        _POOL.putconn(conn)

def warmup_postgres():
    conn = postgres_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT flight_id FROM flights LIMIT 1")
        cur.fetchone()
        conn.commit()
        return None
    finally:
        cur.close()
        _put_conn(conn)

def s_postgres_add_flight(cfg, iteration: int):
    flight = cfg["queries"]["insert_flight"]["flights"][iteration - 1]
    cols = [
        "year", "month", "day_of_month", "day_of_week", "fl_date",
        "op_unique_carrier", "op_carrier_fl_num", "origin", "dest",
        "crs_dep_time", "crs_arr_time", "crs_elapsed_time", "distance"
    ]
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f"INSERT INTO flights ({', '.join(cols)}) VALUES ({placeholders}) RETURNING flight_id"

    conn = postgres_conn()
    cur = conn.cursor()

    cur.execute("SELECT carrier_code FROM airline LIMIT 1;")
    row = cur.fetchone()
    carrier_code = row[0] if row else flight["op_unique_carrier"]

    vals = [
        int(flight["year"]),
        int(flight["month"]),
        int(flight["day_of_month"]),
        int(flight["day_of_week"]),
        flight["fl_date"],
        carrier_code,  
        str(flight["op_carrier_fl_num"]),
        flight["origin"],
        flight["dest"],
        int(flight.get("crs_dep_time", 0)),
        int(flight.get("crs_arr_time", 0)),
        int(flight.get("crs_elapsed_time", 0)),
        int(flight.get("distance", 0))
    ]

    t0 = time.perf_counter()
    try:
        cur.execute(insert_sql, vals)
        inserted_id = cur.fetchone()[0]
        conn.commit()
        dt = (time.perf_counter() - t0) * 1000
        cfg.setdefault("queries", {}).setdefault("insert_flight", {}).setdefault("_inserted_ids_postgres", []).append(inserted_id)
        return dt, f"inserted_id={inserted_id}"
    finally:
        cur.close()
        _put_conn(conn)

def s_postgres_add_flight_stats(cfg, iteration: int):
    conn = postgres_conn()
    cur = conn.cursor()

    update_flight_cfg = cfg["queries"].setdefault("update_flight", {})
    inserted_ids = update_flight_cfg.get("_inserted_ids_postgres", [])

    if len(inserted_ids) >= iteration:
        flight_id = inserted_ids[iteration - 1]
    else:
        cur.execute("""
            SELECT f.flight_id
            FROM flights f
            LEFT JOIN flights_performance p ON p.flight_id = f.flight_id
            WHERE p.flight_id IS NULL
            ORDER BY f.flight_id DESC
            LIMIT 1;
        """)
        row = cur.fetchone()
        if not row or row[0] is None:
            cur.close()
            _put_conn(conn)
            raise RuntimeError("Brak lotu bez statystyk dla postgres_add_flight_stats")
        flight_id = row[0]

    perf = update_flight_cfg["flight_performance"][iteration - 1]
    delayed_list = update_flight_cfg.get("flights_delayed", [])
    delayed_entry = next(
        (d for d in delayed_list if d.get("flight_index") == (iteration - 1)),
        None
    )

    t0 = time.perf_counter()
    try:
        if delayed_entry:
            cur.execute(
                """
                INSERT INTO flights_delayed (
                    flight_id,
                    carrier_delay,
                    weather_delay,
                    nas_delay,
                    security_delay,
                    late_aircraft_delay
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    int(flight_id),
                    int(delayed_entry.get("carrier_delay", 0)),
                    int(delayed_entry.get("weather_delay", 0)),
                    int(delayed_entry.get("nas_delay", 0)),
                    int(delayed_entry.get("security_delay", 0)),
                    int(delayed_entry.get("late_aircraft_delay", 0)),
                )
            )

        cur.execute(
            """
            INSERT INTO flights_performance (
                flight_id,
                dep_time,
                dep_delay,
                taxi_out,
                wheels_off,
                wheels_on,
                taxi_in,
                arr_time,
                arr_delay,
                actual_elapsed_time,
                air_time,
                diverted,
                delay_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (flight_id) DO NOTHING
            """,
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
                flight_id if delayed_entry else None,
            )
        )

        cur.execute(
            """
            INSERT INTO flight_status (flight_id, performance_id, cancellation_id)
            VALUES (%s, %s, %s)
            """,
            (int(flight_id), int(flight_id), None)
        )

        conn.commit()
        dt = (time.perf_counter() - t0) * 1000
        note = f"flight_id={flight_id}, perf_inserted=1"
        if delayed_entry:
            note += ", delayed_inserted=1"
        return dt, note

    finally:
        cur.close()
        _put_conn(conn)

def s_postgres_top_routes_month(cfg, iteration: int):
    month = iteration
    limit = int(cfg["queries"]["top_routes_month"]["limit"])

    conn = postgres_conn()
    cur = conn.cursor()
    try:
        sql_q = (
            "SELECT f.origin, f.dest, COUNT(*) AS flights_count "
            "FROM flights f "
            "WHERE f.month = %s "
            "GROUP BY f.origin, f.dest "
            "ORDER BY flights_count DESC "
            "LIMIT %s"
        )
        t0 = time.perf_counter()
        cur.execute(sql_q, (month, limit))
        rows = cur.fetchall()
        conn.commit()
        dt = (time.perf_counter() - t0) * 1000

        entries = []
        for origin, dest, count in rows:
            entries.append(f"{origin}-{dest}({count})")

        note = ";".join(entries) if entries else "no_results"
        return dt, note
    finally:
        cur.close()
        _put_conn(conn)

def s_postgres_histogram_arr_delay(cfg, iteration: int):
    bins = cfg["queries"]["histogram_arr_delay"]["bins"]
    try:
        bins = [int(b) for b in bins]
    except Exception:
        return 0.0, "invalid_bins"

    if len(bins) < 2:
        return 0.0, "buckets=0"

    parts = []
    params = []
    for i in range(len(bins) - 1):
        a = bins[i]; b = bins[i + 1]
        parts.append(f"SUM(CASE WHEN p.arr_delay >= %s AND p.arr_delay < %s THEN 1 ELSE 0 END) AS b{i}")
        params.extend([a, b])

    parts.append("SUM(CASE WHEN p.arr_delay < %s OR p.arr_delay >= %s OR p.arr_delay IS NULL THEN 1 ELSE 0 END) AS other")
    params.extend([bins[0], bins[-1]])

    sql_q = "SELECT " + ", ".join(parts) + " FROM flights_performance p"

    conn = postgres_conn()
    cur = conn.cursor()
    t0 = time.perf_counter()
    try:
        cur.execute(sql_q, tuple(params))
        row = cur.fetchone()
        conn.commit()
        dt = (time.perf_counter() - t0) * 1000
        num_buckets = len(bins)
        return dt, f"buckets={num_buckets}, total_in_first={row[0] if row else 0}"
    finally:
        cur.close()
        _put_conn(conn)

def s_postgres_find_flights_route_range_with_stats(cfg, iteration: int):
    idx = iteration - 1
    route = cfg["queries"]["find_all_flights_on_route"]["routes"][idx]
    origin = route.get("origin")
    dest = route.get("dest")
    date_from = route.get("date_from")
    date_to = route.get("date_to")
    limit = int(cfg.get("queries", {}).get("find_all_flights_on_route", {}).get("limit", 1000))

    sql_q = (
        "SELECT f.flight_id, f.fl_date, f.op_unique_carrier, f.op_carrier_fl_num, f.origin, f.dest, "
        "p.dep_time, p.dep_delay, p.arr_time, p.arr_delay, p.actual_elapsed_time, p.air_time, p.diverted, "
        "d.carrier_delay, d.weather_delay, d.nas_delay, d.security_delay, d.late_aircraft_delay, "
        "c.cancellation_code "
        "FROM flights f "
        "LEFT JOIN flights_performance p ON f.flight_id = p.flight_id "
        "LEFT JOIN flights_delayed d ON p.delay_id = d.flight_id "
        "LEFT JOIN flights_cancelled c ON f.flight_id = c.flight_id "
        "WHERE f.origin = %s AND f.dest = %s AND f.fl_date BETWEEN %s AND %s "
        "LIMIT %s"
    )

    conn = postgres_conn()
    cur = conn.cursor()
    t0 = time.perf_counter()
    try:
        cur.execute(sql_q, (origin, dest, date_from, date_to, limit))
        rows = cur.fetchall()
        conn.commit()
        dt = (time.perf_counter() - t0) * 1000

        count = len(rows)
        note = f"count={count}"
        return dt, note
    finally:
        cur.close()
        _put_conn(conn)

def s_postgres_get_flight_with_stats(cfg, iteration: int):
    raise NotImplemented

def s_postgres_rank_punctual_airlines(cfg, iteration: int):
    limit = int(cfg["queries"]["airlines_ranking"]["limit"])
    cancellation_weight = float(cfg["queries"]["airlines_ranking"]["cancellation_weight"])
    ranking_for_month = iteration

    sql_q = (
        "SELECT f.op_unique_carrier AS carrier, "
        "       AVG(p.arr_delay) AS avg_arr_delay, "
        "       SUM(CASE WHEN c.flight_id IS NOT NULL THEN 1 ELSE 0 END) AS cancelled_count, "
        "       COUNT(f.flight_id) AS total_flights, "
        "       (COALESCE(AVG(p.arr_delay), 0) + (SUM(CASE WHEN c.flight_id IS NOT NULL THEN 1 ELSE 0 END) * %s / GREATEST(COUNT(f.flight_id),1)) * 100) AS score "
        "FROM flights f "
        "LEFT JOIN flights_performance p ON f.flight_id = p.flight_id "
        "LEFT JOIN flights_cancelled c ON f.flight_id = c.flight_id "
        "WHERE f.month = %s "
        "GROUP BY f.op_unique_carrier "
        "HAVING COUNT(f.flight_id) > 0 "
        "ORDER BY score ASC "
        "LIMIT %s"
    )

    conn = postgres_conn()
    cur = conn.cursor()
    t0 = time.perf_counter()
    try:
        cur.execute(sql_q, (cancellation_weight, ranking_for_month, limit))
        rows = cur.fetchall()
        conn.commit()
        dt = (time.perf_counter() - t0) * 1000
        note = ("month=" + str(ranking_for_month) + ", most_punctual=" + rows[0][0]) if rows else "no_results"
        return dt, note
    finally:
        cur.close()
        _put_conn(conn)

SCENARIOS_POSTGRES = [
    ("postgres_add_flight", s_postgres_add_flight),
    ("postgres_add_flight_stats", s_postgres_add_flight_stats),
    ("postgres_top_routes_month", s_postgres_top_routes_month),
    ("postgres_histogram_arr_delay", s_postgres_histogram_arr_delay),
    ("postgres_find_route_with_stats", s_postgres_find_flights_route_range_with_stats),
    # ("postgres_get_flight_with_stats", s_postgres_get_flight_with_stats),
    ("postgres_rank_punctual_airlines", s_postgres_rank_punctual_airlines),
]

def run_postgres(cfg, dataset_name: str, dataset_size: int):
    # TODO: create postgres db container and insert data according to dataset_Size
    warmup_postgres()
    for name, fn in SCENARIOS_POSTGRES:
        for r in range(1, int(cfg["repeats"]) + 1):
            dt, notes = fn(cfg, r)
            log_result("postgres", dataset_name, name, r, dt, notes)
            print(f"[postgres][{name}][run={r}] {dt:.2f} ms :: {notes}")
    # TODO: delete posgres database container (with db data)
