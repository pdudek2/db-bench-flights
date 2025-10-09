import os, time, csv, yaml
from datetime import datetime, timedelta
from pathlib import Path

RESULTS_PATH = Path("/app/results/results.csv")

def load_cfg():
    with open("bench_config.yml", "r") as f:
        return yaml.safe_load(f)

def ensure_results_header():
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not RESULTS_PATH.exists():
        with open(RESULTS_PATH, "w", newline="") as f:
            csv.writer(f).writerow(["ts","db","dataset","scenario","repeat","elapsed_ms","notes"])

def log_result(db, dataset, scenario, repeat, ms, notes=""):
    with open(RESULTS_PATH, "a", newline="") as f:
        csv.writer(f).writerow([datetime.utcnow().isoformat(), db, dataset, scenario, repeat, round(ms,2), notes])

from pymongo import MongoClient

def mongo_client():
    host = os.getenv("MONGO_HOST", "mongodb")  
    uri = f"mongodb://{host}:27017/?retryWrites=false"
    return MongoClient(uri, serverSelectionTimeoutMS=5000)

def s_mongo_read_by_carrier_day(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    q = cfg["queries"]["read_by_carrier_day"]
    query = {"op_unique_carrier": q["carrier"], "fl_date": {"$gte": q["date_from"], "$lte": q["date_to"]}}
    t0 = time.perf_counter()
    docs = list(col.find(query).limit(int(q["limit"])))
    dt = (time.perf_counter()-t0)*1000
    return dt, f"found={len(docs)}"

def s_mongo_top_routes_month(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    month = cfg["queries"]["top_routes_month"]["month"] 
    y, m = map(int, month.split("-"))
    start = f"{y}-{m:02d}-01"
    end   = f"{y+1}-01-01" if m == 12 else f"{y}-{m+1:02d}-01"
    pipeline = [
        {"$match": {"fl_date": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": {"origin":"$origin","dest":"$dest"},
                    "avg_arr_delay": {"$avg":"$arr_delay"},
                    "cnt": {"$sum":1}}},
        {"$sort": {"avg_arr_delay": -1}},
        {"$limit": 10}
    ]
    t0 = time.perf_counter()
    res = list(col.aggregate(pipeline, allowDiskUse=True))
    dt = (time.perf_counter()-t0)*1000
    return dt, f"rows={len(res)}"

def s_mongo_histogram_arr_delay(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    bins = cfg["queries"]["histogram_arr_delay"]["bins"]
    pipeline = [
        {"$bucket": {
            "groupBy": "$arr_delay",
            "boundaries": bins,
            "default": "other",
            "output": {"count": {"$sum": 1}}
        }}
    ]
    t0 = time.perf_counter()
    res = list(col.aggregate(pipeline, allowDiskUse=True))
    dt = (time.perf_counter()-t0)*1000
    return dt, f"buckets={len(res)}"

def s_mongo_insert_batch(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    n = int(cfg["crud"]["sample_size_for_writes"])
    sample = [{
        "fl_date":"2024-02-01",
        "op_unique_carrier":"ZZ",
        "op_carrier_fl_num":i,
        "origin":"AAA","dest":"BBB",
        "arr_delay":i%60,"dep_delay":i%30
    } for i in range(n)]
    t0 = time.perf_counter()
    col.insert_many(sample, ordered=False)
    dt = (time.perf_counter()-t0)*1000
    return dt, f"inserted={n}"

def s_mongo_update_many(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    t0 = time.perf_counter()
    res = col.update_many({"op_unique_carrier":"ZZ"}, {"$inc": {"arr_delay": 1}})
    dt = (time.perf_counter()-t0)*1000
    return dt, f"matched={res.matched_count}, modified={res.modified_count}"

def s_mongo_delete_many(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    t0 = time.perf_counter()
    res = col.delete_many({"op_unique_carrier":"ZZ"})
    dt = (time.perf_counter()-t0)*1000
    return dt, f"deleted={res.deleted_count}"

SCENARIOS_MONGO = [
    ("mongo_read_by_carrier_day", s_mongo_read_by_carrier_day),
    ("mongo_top_routes_month",    s_mongo_top_routes_month),
    ("mongo_histogram_arr_delay", s_mongo_histogram_arr_delay),
    ("mongo_insert_batch",        s_mongo_insert_batch),
    ("mongo_update_many",         s_mongo_update_many),
    ("mongo_delete_many",         s_mongo_delete_many),
]

def run_mongo(cfg):
    for name, fn in SCENARIOS_MONGO:
        for r in range(1, int(cfg["repeats"])+1):
            ms, notes = fn(cfg)
            log_result("mongo", cfg["dataset_name"], name, r, ms, notes)
            print(f"[mongo][{name}][run={r}] {ms:.2f} ms :: {notes}")

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

def cass_client():
    host = "cassandra"
    port = 9042
    cluster = Cluster([host], port=port)
    sess = cluster.connect("flightsks")
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
    st = s.prepare("SELECT origin,dest,arr_delay FROM flights WHERE op_unique_carrier=? AND fl_date=? LIMIT ?")
    for d in days:
        if got >= limit_total:
            break
        rows = s.execute(st, (carrier, d, max(0, limit_total-got)))
        cnt = sum(1 for _ in rows)
        got += cnt
    dt = (time.perf_counter() - t0)*1000
    return dt, f"found={got}"

def s_cass_top_routes_month(cfg):
    s = cass_client()
    month = cfg["queries"]["top_routes_month"]["month"]  
    y, m = map(int, month.split("-"))
    start = f"{y}-{m:02d}-01"
    end   = f"{y+1}-01-01" if m == 12 else f"{y}-{m+1:02d}-01"
    stmt = SimpleStatement(
        "SELECT origin,dest,arr_delay FROM flights "
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
        ((k, (tot/cnt if cnt else 0.0), cnt) for k,(tot,cnt) in agg.items()),
        key=lambda x: x[1], reverse=True
    )[:10]
    dt = (time.perf_counter() - t0)*1000
    return dt, f"rows={len(top)} scanned={n}"

def s_cass_histogram_arr_delay(cfg):
    s = cass_client()
    bins = cfg["queries"]["histogram_arr_delay"]["bins"]
    # pełny skan; bez IS NOT NULL i bez ALLOW FILTERING
    stmt = SimpleStatement("SELECT arr_delay FROM flights")
    t0 = time.perf_counter()
    counts = [0] * (len(bins))  # ostatni kubełek = "other"
    for row in s.execute(stmt):
        v = row.arr_delay
        if v is None:
            continue
        v = float(v)
        placed = False
        for i in range(len(bins)-1):
            if bins[i] <= v < bins[i+1]:
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
    # zawężamy do miesiąca jak w top_routes_month
    month = cfg["queries"]["top_routes_month"]["month"]
    y, m = map(int, month.split("-"))
    start = f"{y}-{m:02d}-01"
    end   = f"{y+1}-01-01" if m == 12 else f"{y}-{m+1:02d}-01"
    # Cass wymaga ALLOW FILTERING dla zakresu po fl_date (u Ciebie fl_date jest TEXT)
    stmt = SimpleStatement(
        "SELECT arr_delay FROM flights WHERE fl_date >= %s AND fl_date < %s ALLOW FILTERING"
    )
    t0 = time.perf_counter()
    counts = [0]*len(bins)
    for row in s.execute(stmt, (start, end)):
        v = row.arr_delay
        if v is None: 
            continue
        v = float(v)
        placed = False
        for i in range(len(bins)-1):
            if bins[i] <= v < bins[i+1]:
                counts[i] += 1
                placed = True
                break
        if not placed:
            counts[-1] += 1
    dt = (time.perf_counter()-t0)*1000
    return dt, f"buckets={len(counts)}"


SCENARIOS_CASS = [
    ("cass_read_by_carrier_day", s_cass_read_by_carrier_day),
    ("cass_top_routes_month",    s_cass_top_routes_month),
    ("cass_histogram_arr_delay", s_cass_histogram_arr_delay),
    ("cass_histogram_arr_delay_month", s_cass_histogram_arr_delay_month),
]

def run_cassandra(cfg):
    for name, fn in SCENARIOS_CASS:
        for r in range(1, int(cfg["repeats"])+1):
            ms, notes = fn(cfg)
            log_result("cassandra", cfg["dataset_name"], name, r, ms, notes)
            print(f"[cassandra][{name}][run={r}] {ms:.2f} ms :: {notes}")

if __name__ == "__main__":
    cfg = load_cfg()
    ensure_results_header()
    db = str(cfg.get("db", "mongo")).lower()
    if db == "mongo":
        run_mongo(cfg)
    elif db == "cassandra":
        run_cassandra(cfg)
    else:
        raise SystemExit(f"Unknown db in bench_config.yml: {db}")
