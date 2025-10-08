import os, time, csv, yaml
from datetime import datetime
from pymongo import MongoClient
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

def mongo_client():
    host = os.getenv("MONGO_HOST", "mongodb")
    uri = f"mongodb://{host}:27017/?retryWrites=false"
    return MongoClient(uri, serverSelectionTimeoutMS=5000)

def s_mongo_read_by_carrier_day(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    q = cfg["queries"]["read_by_carrier_day"]
    query = {"op_unique_carrier": q["carrier"], "fl_date": {"$gte": q["date_from"], "$lte": q["date_to"]}}
    t0 = time.perf_counter(); docs = list(col.find(query).limit(q["limit"])); dt = (time.perf_counter()-t0)*1000
    return dt, f"found={len(docs)}"

def s_mongo_top_routes_month(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    month = cfg["queries"]["top_routes_month"]["month"]
    pipeline = [
        {"$match": {"fl_date": {"$regex": f"^{month}"}}},
        {"$group": {"_id": {"origin":"$origin","dest":"$dest"}, "avg_arr_delay": {"$avg":"$arr_delay"}, "cnt": {"$sum":1}}},
        {"$sort": {"avg_arr_delay": -1}},
        {"$limit": 10}
    ]
    t0 = time.perf_counter(); res = list(col.aggregate(pipeline, allowDiskUse=True)); dt = (time.perf_counter()-t0)*1000
    return dt, f"rows={len(res)}"

def s_mongo_histogram_arr_delay(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    bins = cfg["queries"]["histogram_arr_delay"]["bins"]
    pipeline = [{"$bucket": {"groupBy": "$arr_delay", "boundaries": bins, "default": "other", "output": {"count": {"$sum": 1}}}}]
    t0 = time.perf_counter(); res = list(col.aggregate(pipeline, allowDiskUse=True)); dt = (time.perf_counter()-t0)*1000
    return dt, f"buckets={len(res)}"

def s_mongo_insert_batch(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    n = int(cfg["crud"]["sample_size_for_writes"])
    sample = [{"fl_date":"2024-02-01","op_unique_carrier":"ZZ","op_carrier_fl_num":i,"origin":"AAA","dest":"BBB","arr_delay":i%60,"dep_delay":i%30} for i in range(n)]
    t0 = time.perf_counter(); col.insert_many(sample, ordered=False); dt = (time.perf_counter()-t0)*1000
    return dt, f"inserted={n}"

def s_mongo_update_many(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    t0 = time.perf_counter(); res = col.update_many({"op_unique_carrier":"ZZ"}, {"$inc": {"arr_delay": 1}}); dt = (time.perf_counter()-t0)*1000
    return dt, f"matched={res.matched_count}, modified={res.modified_count}"

def s_mongo_delete_many(cfg):
    c = mongo_client(); col = c["flightsdb"]["flights"]
    t0 = time.perf_counter(); res = col.delete_many({"op_unique_carrier":"ZZ"}); dt = (time.perf_counter()-t0)*1000
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

if __name__ == "__main__":
    cfg = load_cfg()
    ensure_results_header()
    run_mongo(cfg)
