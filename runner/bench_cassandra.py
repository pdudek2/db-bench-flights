import time
from datetime import datetime, timedelta, date
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

from bench_common import log_result


def cass_client():
    host = "cassandra"
    port = 9042
    cluster = Cluster([host], port=port)
    sess = cluster.connect("flights")
    sess.default_timeout = 20
    return sess

def reset_cassandra():
    s = cass_client()
    s.execute("TRUNCATE flights_by_route_day;")
    s.execute("TRUNCATE flights_by_carrier_day;")

import csv

def import_to_cassandra(file_name):
    print(f"\n[IMPORTING] Importing {file_name} into Cassandra...")

    s = cass_client()

    insert_route = s.prepare(
        """
        INSERT INTO flights_by_route_day (
            origin, dest, fl_date, dep_time,
            op_unique_carrier, op_carrier_fl_num,
            arr_delay, dep_delay, distance,
            cancelled, diverted
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    insert_carrier = s.prepare(
        """
        INSERT INTO flights_by_carrier_day (
            op_unique_carrier, fl_date, dep_time,
            origin, dest, op_carrier_fl_num,
            arr_delay, dep_delay, cancelled, diverted
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    with open(file_name, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            origin = row.get("origin")
            dest = row.get("dest")

            fl_date_str = row.get("fl_date")  # np. "2024-01-15"
            if not fl_date_str:
                continue
            fl_date = _parse_date(fl_date_str)

            # uwaga: w danych może być crs_dep_time albo dep_time
            dep_time_raw = row.get("crs_dep_time") or row.get("dep_time") or "0"
            try:
                dep_time = int(dep_time_raw)
            except ValueError:
                dep_time = 0

            carrier = row.get("op_unique_carrier")
            fl_num_raw = row.get("op_carrier_fl_num") or "0"
            try:
                fl_num = int(fl_num_raw)
            except ValueError:
                fl_num = 0

            def to_int(name):
                v = row.get(name)
                if v in (None, ""):
                    return 0
                try:
                    return int(float(v))
                except ValueError:
                    return 0

            arr_delay = to_int("arr_delay")
            dep_delay = to_int("dep_delay")
            distance = to_int("distance")
            cancelled = to_int("cancelled")
            diverted = to_int("diverted")

            s.execute(
                insert_route,
                (
                    origin, dest, fl_date, dep_time,
                    carrier, fl_num,
                    arr_delay, dep_delay, distance,
                    cancelled, diverted,
                ),
            )

            s.execute(
                insert_carrier,
                (
                    carrier, fl_date, dep_time,
                    origin, dest, fl_num,
                    arr_delay, dep_delay, cancelled, diverted,
                ),
            )

    print("[IMPORTING][cassandra] done.")

def daterange_strs(date_from, date_to):
    d0 = datetime.fromisoformat(date_from)
    d1 = datetime.fromisoformat(date_to)
    cur = d0
    out = []
    while cur <= d1:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


def _parse_date(iso_str: str) -> date:
    return datetime.fromisoformat(iso_str).date()



def s_cass_add_flight(cfg, iteration: int):
    """
    Analog mysql_add_flight:
    bierzemy kolejne flight sample z queries.insert_flight.flights
    i wstawiamy do flights_by_route_day + flights_by_carrier_day.
    """
    s = cass_client()
    insert_cfg = cfg["queries"]["insert_flight"]
    flights = insert_cfg.get("flights", [])
    if not flights:
        return 0.0, "no_flights_in_config"

    flight = flights[(iteration - 1) % len(flights)]

    fl_date_str = flight["fl_date"]
    fl_date = _parse_date(fl_date_str)

    origin = str(flight["origin"])
    dest = str(flight["dest"])
    carrier = str(flight["op_unique_carrier"])
    fl_num = int(flight["op_carrier_fl_num"])
    dep_time = int(flight.get("crs_dep_time", 0))
    distance = int(flight.get("distance", 0))

    arr_delay = 0
    dep_delay = 0
    cancelled = 0
    diverted = 0

    t0 = time.perf_counter()

    s.execute(
        """
        INSERT INTO flights_by_route_day (
            origin, dest, fl_date, dep_time,
            op_unique_carrier, op_carrier_fl_num,
            arr_delay, dep_delay, distance,
            cancelled, diverted
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            origin, dest, fl_date, dep_time,
            carrier, fl_num,
            arr_delay, dep_delay, distance,
            cancelled, diverted,
        ),
    )

    s.execute(
        """
        INSERT INTO flights_by_carrier_day (
            op_unique_carrier, fl_date, dep_time,
            origin, dest, op_carrier_fl_num,
            arr_delay, dep_delay, cancelled, diverted
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            carrier, fl_date, dep_time,
            origin, dest, fl_num,
            arr_delay, dep_delay,
            cancelled, diverted,
        ),
    )

    dt = (time.perf_counter() - t0) * 1000
    return dt, "OK"


def s_cass_add_flight_stats(cfg, iteration: int):
    """
    Analog mysql_add_flight_stats:
    łączy dane z insert_flight + update_flight.flight_performance
    i wstawia kolejne wiersze z arr_delay / dep_delay itp.
    """
    s = cass_client()

    insert_cfg = cfg["queries"]["insert_flight"]
    flights = insert_cfg.get("flights", [])
    update_cfg = cfg["queries"]["update_flight"]
    perf_list = update_cfg.get("flight_performance", [])

    if not flights or not perf_list:
        return 0.0, "no_flights_or_performance"

    flight = flights[(iteration - 1) % len(flights)]
    perf = perf_list[(iteration - 1) % len(perf_list)]

    fl_date_str = flight["fl_date"]
    fl_date = _parse_date(fl_date_str)

    origin = str(flight["origin"])
    dest = str(flight["dest"])
    carrier = str(flight["op_unique_carrier"])
    fl_num = int(flight["op_carrier_fl_num"])
    distance = int(flight.get("distance", 0))

    dep_time = int(perf.get("dep_time", 0))
    arr_delay = int(perf.get("arr_delay", 0))
    dep_delay = int(perf.get("dep_delay", 0))
    cancelled = 1 if perf.get("cancelled") else 0
    diverted = 1 if perf.get("diverted") else 0

    t0 = time.perf_counter()

    s.execute(
        """
        INSERT INTO flights_by_route_day (
            origin, dest, fl_date, dep_time,
            op_unique_carrier, op_carrier_fl_num,
            arr_delay, dep_delay, distance,
            cancelled, diverted
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            origin, dest, fl_date, dep_time,
            carrier, fl_num,
            arr_delay, dep_delay, distance,
            cancelled, diverted,
        ),
    )

    s.execute(
        """
        INSERT INTO flights_by_carrier_day (
            op_unique_carrier, fl_date, dep_time,
            origin, dest, op_carrier_fl_num,
            arr_delay, dep_delay, cancelled, diverted
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            carrier, fl_date, dep_time,
            origin, dest, fl_num,
            arr_delay, dep_delay,
            cancelled, diverted,
        ),
    )

    dt = (time.perf_counter() - t0) * 1000
    note = f"carrier={carrier}, route={origin}-{dest}"
    return dt, note


def s_cass_top_routes_month(cfg, iteration: int):
    """
    Analog top_routes_month – korzystamy z flights_by_route_day,
    filtrujemy po fl_date (zakres miesiąca) i liczymy średni arr_delay.
    """
    s = cass_client()
    month_cfg = cfg["queries"]["top_routes_month"]["month"]
    y, m = map(int, month_cfg.split("-"))
    start = datetime(y, m, 1).date()
    end = datetime(y + 1, 1, 1).date() if m == 12 else datetime(y, m + 1, 1).date()

    stmt = SimpleStatement(
        "SELECT origin, dest, arr_delay FROM flights_by_route_day "
        "WHERE fl_date >= %s AND fl_date < %s ALLOW FILTERING"
    )

    t0 = time.perf_counter()
    agg = {}
    scanned = 0

    for row in s.execute(stmt, (start, end)):
        key = (row.origin, row.dest)
        if row.arr_delay is not None:
            tot, cnt = agg.get(key, (0.0, 0))
            tot += float(row.arr_delay)
            cnt += 1
            agg[key] = (tot, cnt)
            scanned += 1

    top = sorted(
        (
            (k, (tot / cnt if cnt else 0.0), cnt)
            for k, (tot, cnt) in agg.items()
        ),
        key=lambda x: x[1],
        reverse=True,
    )[:10]

    dt = (time.perf_counter() - t0) * 1000
    return dt, f"rows={len(top)} scanned={scanned}"


def s_cass_histogram_arr_delay(cfg, iteration: int):
    """
    Analog histogram_arr_delay – globalnie po flights_by_route_day.
    """
    s = cass_client()
    bins = cfg["queries"]["histogram_arr_delay"]["bins"]
    stmt = SimpleStatement("SELECT arr_delay FROM flights_by_route_day")

    t0 = time.perf_counter()
    counts = [0] * len(bins)

    for row in s.execute(stmt):
        v = row.arr_delay
        if v is None:
            continue
        v = float(v)
        placed = False
        for i in range(len(bins) - 1):
            if bins[i] <= v < bins[i + 1]:
                counts[i] += 1
                placed = True
                break
        if not placed:
            counts[-1] += 1

    dt = (time.perf_counter() - t0) * 1000
    return dt, f"buckets={len(counts)}"


def s_cass_find_route_with_stats(cfg, iteration: int):
    """
    Analog find_flights_route_range_with_stats:
    bazujemy na flights_by_route_day – arr_delay, dep_delay, cancelled, diverted
    mamy w jednym wierszu, więc "statystyki" są od razu z tabeli.
    """
    s = cass_client()
    q = cfg["queries"]["find_all_flights_on_route"]
    routes = q.get("routes", [])
    if not routes:
        return 0.0, "no_routes_in_config"

    route = routes[(iteration - 1) % len(routes)]
    origin = route.get("origin")
    dest = route.get("dest")
    date_from = _parse_date(route.get("date_from"))
    date_to = _parse_date(route.get("date_to"))
    limit = int(q.get("limit", 1000))

    stmt = SimpleStatement(
        """
        SELECT origin, dest, fl_date, dep_time,
               op_unique_carrier, op_carrier_fl_num,
               arr_delay, dep_delay, cancelled, diverted
        FROM flights_by_route_day
        WHERE origin = %s AND dest = %s
          AND fl_date >= %s AND fl_date <= %s
        ALLOW FILTERING
        """
    )

    t0 = time.perf_counter()
    count = 0
    for row in s.execute(stmt, (origin, dest, date_from, date_to)):
        count += 1
        if count >= limit:
            break

    dt = (time.perf_counter() - t0) * 1000
    return dt, f"count={count}"


def s_cass_rank_punctual_airlines(cfg, iteration: int):
    """
    Analog mysql_rank_punctual_airlines:
    - dla miesiąca = iteration (1..12) liczymy:
      avg_arr_delay + (cancelled_count * cancellation_weight / total_flights) * 100
    na podstawie flights_by_route_day (pole cancelled).
    """
    s = cass_client()
    rank_cfg = cfg["queries"]["airlines_ranking"]
    limit = int(rank_cfg["limit"])
    cancellation_weight = float(rank_cfg["cancellation_weight"])

    month = iteration
    year = 2018  
    start = datetime(year, month, 1).date()
    end = datetime(year + 1, 1, 1).date() if month == 12 else datetime(year, month + 1, 1).date()

    stmt = SimpleStatement(
        """
        SELECT op_unique_carrier, arr_delay, cancelled
        FROM flights_by_route_day
        WHERE fl_date >= %s AND fl_date < %s
        ALLOW FILTERING
        """
    )

    t0 = time.perf_counter()
    stats = {}  

    for row in s.execute(stmt, (start, end)):
        carrier = row.op_unique_carrier or "UNK"
        if carrier not in stats:
            stats[carrier] = {"sum_delay": 0.0, "total": 0, "cancelled": 0}

        if row.arr_delay is not None:
            stats[carrier]["sum_delay"] += float(row.arr_delay)
            stats[carrier]["total"] += 1

        if row.cancelled is not None and int(row.cancelled) != 0:
            stats[carrier]["cancelled"] += 1

    scores = []
    for carrier, s_data in stats.items():
        total = max(s_data["total"], 1)
        avg_delay = s_data["sum_delay"] / total if s_data["total"] > 0 else 0.0
        cancel_component = (s_data["cancelled"] * cancellation_weight / total) * 100.0
        score = avg_delay + cancel_component
        scores.append((carrier, score))

    if not scores:
        dt = (time.perf_counter() - t0) * 1000
        return dt, f"month={month}, no_results"

    scores.sort(key=lambda x: x[1])
    best_carrier = scores[0][0]
    dt = (time.perf_counter() - t0) * 1000
    note = f"month={month}, most_punctual={best_carrier}"
    return dt, note



def s_cass_read_by_carrier_day(cfg, iteration: int):
    s = cass_client()
    q = cfg["queries"]["read_by_carrier_day"]
    carrier = q["carrier"]
    days = daterange_strs(q["date_from"], q["date_to"])
    limit_total = int(q["limit"])
    got = 0

    t0 = time.perf_counter()
    st = s.prepare(
        "SELECT origin, dest, arr_delay "
        "FROM flights_by_carrier_day "
        "WHERE op_unique_carrier = ? AND fl_date = ?"
    )

    for d in days:
        if got >= limit_total:
            break
        fl_date = _parse_date(d)
        rows = s.execute(st, (carrier, fl_date))
        for _ in rows:
            got += 1
            if got >= limit_total:
                break

    dt = (time.perf_counter() - t0) * 1000
    return dt, f"found={got}"


def s_cass_histogram_arr_delay_month(cfg, iteration: int):
    s = cass_client()
    bins = cfg["queries"]["histogram_arr_delay"]["bins"]
    month_cfg = cfg["queries"]["top_routes_month"]["month"]
    y, m = map(int, month_cfg.split("-"))
    start = datetime(y, m, 1).date()
    end = datetime(y + 1, 1, 1).date() if m == 12 else datetime(y, m + 1, 1).date()

    stmt = SimpleStatement(
        "SELECT arr_delay FROM flights_by_route_day "
        "WHERE fl_date >= %s AND fl_date < %s ALLOW FILTERING"
    )

    t0 = time.perf_counter()
    counts = [0] * len(bins)

    for row in s.execute(stmt, (start, end)):
        v = row.arr_delay
        if v is None:
            continue
        v = float(v)
        placed = False
        for i in range(len(bins) - 1):
            if bins[i] <= v < bins[i + 1]:
                counts[i] += 1
                placed = True
                break
        if not placed:
            counts[-1] += 1

    dt = (time.perf_counter() - t0) * 1000
    return dt, f"buckets={len(counts)}"


SCENARIOS_CASS = [
    ("cass_add_flight", s_cass_add_flight),
    ("cass_add_flight_stats", s_cass_add_flight_stats),
    ("cass_top_routes_month", s_cass_top_routes_month),
    ("cass_histogram_arr_delay", s_cass_histogram_arr_delay),
    ("cass_find_route_with_stats", s_cass_find_route_with_stats),
    ("cass_rank_punctual_airlines", s_cass_rank_punctual_airlines),

    ("cass_read_by_carrier_day", s_cass_read_by_carrier_day),
    ("cass_histogram_arr_delay_month", s_cass_histogram_arr_delay_month),
]


def run_cassandra(cfg, dataset_size: int, dataset_name: str):
    for name, fn in SCENARIOS_CASS:
        for r in range(1, int(cfg["repeats"]) + 1):
            ms, notes = fn(cfg, r)
            log_result("cassandra", dataset_name, name, r, ms, notes)
            print(f"[cassandra][{name}][run={r}] {ms:.2f} ms :: {notes}")
