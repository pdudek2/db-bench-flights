import time
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

from bench_common import log_result


def cass_client():
    host = "cassandra"
    port = 9042
    cluster = Cluster([host], port=port)
    # keyspace zgodny ze schema.cql
    sess = cluster.connect("flights")
    sess.default_timeout = 20
    return sess


def daterange_strs(date_from, date_to):
    d0 = datetime.fromisoformat(date_from)
    d1 = datetime.fromisoformat(date_to)
    cur = d0
    out = []
    while cur <= d1:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


def s_cass_read_by_carrier_day(cfg):
    s = cass_client()
    q = cfg["queries"]["read_by_carrier_day"]
    carrier = q["carrier"]
    days = daterange_strs(q["date_from"], q["date_to"])
    limit_total = int(q["limit"])
    got = 0

    t0 = time.perf_counter()
    # tabela zgodna ze schema: flights_by_carrier_day
    st = s.prepare(
        "SELECT origin, dest, arr_delay "
        "FROM flights_by_carrier_day "
        "WHERE op_unique_carrier = ? AND fl_date = ? LIMIT ?"
    )

    for d in days:
        if got >= limit_total:
            break
        rows = s.execute(st, (carrier, d, max(0, limit_total - got)))
        cnt = sum(1 for _ in rows)
        got += cnt

    dt = (time.perf_counter() - t0) * 1000
    return dt, f"found={got}"


def s_cass_top_routes_month(cfg):
    s = cass_client()
    month = cfg["queries"]["top_routes_month"]["month"]
    y, m = map(int, month.split("-"))
    start = f"{y}-{m:02d}-01"
    end = f"{y+1}-01-01" if m == 12 else f"{y}-{m+1:02d}-01"

    # korzystamy z flights_by_route_day
    stmt = SimpleStatement(
        "SELECT origin, dest, arr_delay FROM flights_by_route_day "
        "WHERE fl_date >= %s AND fl_date < %s ALLOW FILTERING"
    )

    t0 = time.perf_counter()
    agg = {}
    n = 0

    for row in s.execute(stmt, (start, end)):
        key = (row.origin, row.dest)
        if row.arr_delay is not None:
            tot, cnt = agg.get(key, (0.0, 0))
            tot += float(row.arr_delay)
            cnt += 1
            agg[key] = (tot, cnt)
            n += 1

    top = sorted(
        ((k, (tot / cnt if cnt else 0.0), cnt) for k, (tot, cnt) in agg.items()),
        key=lambda x: x[1],
        reverse=True,
    )[:10]

    dt = (time.perf_counter() - t0) * 1000
    return dt, f"rows={len(top)} scanned={n}"


def s_cass_histogram_arr_delay(cfg):
    s = cass_client()
    bins = cfg["queries"]["histogram_arr_delay"]["bins"]
    # globalny histogram po wszystkich lotach -> flights_by_route_day
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


def s_cass_histogram_arr_delay_month(cfg):
    s = cass_client()
    bins = cfg["queries"]["histogram_arr_delay"]["bins"]
    month = cfg["queries"]["top_routes_month"]["month"]
    y, m = map(int, month.split("-"))
    start = f"{y}-{m:02d}-01"
    end = f"{y+1}-01-01" if m == 12 else f"{y}-{m+1:02d}-01"

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
    ("cass_read_by_carrier_day", s_cass_read_by_carrier_day),
    ("cass_top_routes_month", s_cass_top_routes_month),
    ("cass_histogram_arr_delay", s_cass_histogram_arr_delay),
    ("cass_histogram_arr_delay_month", s_cass_histogram_arr_delay_month),
]


def run_cassandra(cfg, dataset_size: int, dataset_name: str):
    for name, fn in SCENARIOS_CASS:
        for r in range(1, int(cfg["repeats"]) + 1):
            ms, notes = fn(cfg)
            log_result("cassandra", dataset_name, name, r, ms, notes)
            print(f"[cassandra][{name}][run={r}] {ms:.2f} ms :: {notes}")
