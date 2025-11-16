import os, time
from pymongo import MongoClient

from bench_common import log_result


def mongo_client():
    host = os.getenv("MONGO_HOST", "mongodb")
    uri = f"mongodb://{host}:27017/?retryWrites=false"
    return MongoClient(uri, serverSelectionTimeoutMS=5000)

def reset_mongo():
    c = mongo_client()
    db = c["flightsdb"]
    db["flights"].delete_many({})

def s_mongo_add_flight(cfg):
    """
    Analog mysql_add_flight:
    - bierze kolejne sample z queries.insert_flight.flights
    - wrzuca jako dokument do flightsdb.flights
    - zapisuje _id do queries.update_flight._inserted_ids
    """
    c = mongo_client()
    db = c["flightsdb"]
    col = db["flights"]

    insert_cfg = cfg["queries"]["insert_flight"]
    flights = insert_cfg.get("flights", [])
    if not flights:
        return 0.0, "no_flights_in_config"

    idx = insert_cfg.get("_next_idx", 0)
    flight = flights[idx % len(flights)]
    insert_cfg["_next_idx"] = idx + 1

    doc = dict(flight)

    t0 = time.perf_counter()
    res = col.insert_one(doc)
    dt = (time.perf_counter() - t0) * 1000.0

    update_cfg = cfg["queries"].setdefault("update_flight", {})
    inserted_ids = update_cfg.setdefault("_inserted_ids", [])
    inserted_ids.append(res.inserted_id)

    return dt, "OK"


def s_mongo_add_flight_stats(cfg):
    """
    Analog mysql_add_flight_stats:
    - bierze kolejne flight_id z update_flight._inserted_ids
    - bierze odpowiednie dane z flight_performance i flights_delayed
    - wstawia do kolekcji flights_performance, flights_delayed
    """
    c = mongo_client()
    db = c["flightsdb"]
    col_perf = db["flights_performance"]
    col_delayed = db["flights_delayed"]

    update_cfg = cfg["queries"]["update_flight"]

    inserted_ids = update_cfg.get("_inserted_ids", [])
    perf_list = update_cfg.get("flight_performance", [])

    if not perf_list:
        return 0.0, "no_flight_performance"

    idx = update_cfg.get("_stats_idx", 0)
    update_cfg["_stats_idx"] = idx + 1

    if inserted_ids:
        flight_id = inserted_ids[idx % len(inserted_ids)]
    else:
        col_f = db["flights"]
        doc_any = col_f.find_one({}, projection={"_id": 1})
        if not doc_any:
            return 0.0, "no_flights_in_db"
        flight_id = doc_any["_id"]

    perf = perf_list[idx % len(perf_list)]

    delayed_list = update_cfg.get("flights_delayed", [])
    delayed_entry = next(
        (d for d in delayed_list if d.get("flight_index") == (idx % len(perf_list))),
        None
    )

    t0 = time.perf_counter()

    if delayed_entry:
        col_delayed.insert_one({
            "flight_id": flight_id,
            "carrier_delay": int(delayed_entry.get("carrier_delay", 0)),
            "weather_delay": int(delayed_entry.get("weather_delay", 0)),
            "nas_delay": int(delayed_entry.get("nas_delay", 0)),
            "security_delay": int(delayed_entry.get("security_delay", 0)),
            "late_aircraft_delay": int(delayed_entry.get("late_aircraft_delay", 0)),
        })

    col_perf.insert_one({
        "flight_id": flight_id,
        "dep_time": int(perf.get("dep_time", 0)),
        "dep_delay": int(perf.get("dep_delay", 0)),
        "taxi_out": int(perf.get("taxi_out", 0)),
        "wheels_off": int(perf.get("wheels_off", 0)),
        "wheels_on": int(perf.get("wheels_on", 0)),
        "taxi_in": int(perf.get("taxi_in", 0)),
        "arr_time": int(perf.get("arr_time", 0)),
        "arr_delay": int(perf.get("arr_delay", 0)),
        "actual_elapsed_time": int(perf.get("actual_elapsed_time", 0)),
        "air_time": int(perf.get("air_time", 0)),
        "diverted": bool(perf.get("diverted", False)),
    })

    dt = (time.perf_counter() - t0) * 1000.0
    note = f"flight_id={flight_id}, perf_inserted=1"
    if delayed_entry:
        note += ", delayed_inserted=1"
    return dt, note


def s_mongo_find_route_with_stats(cfg):
    """
    Analog mysql_find_flights_route_range_with_stats:
    - wybiera kolejną trasę z queries.find_all_flights_on_route.routes
    - zwraca count wyników z joinem na performance / delayed / cancelled
    """
    c = mongo_client()
    db = c["flightsdb"]
    col_f = db["flights"]

    q = cfg["queries"]["find_all_flights_on_route"]
    routes = q.get("routes", [])
    if not routes:
        return 0.0, "no_routes_in_config"

    idx = q.get("_next_route_idx", 0)
    q["_next_route_idx"] = idx + 1
    route = routes[idx % len(routes)]

    origin = route.get("origin")
    dest = route.get("dest")
    date_from = route.get("date_from")
    date_to = route.get("date_to")
    limit = int(q.get("limit", 1000))

    pipeline = [
        {
            "$match": {
                "origin": origin,
                "dest": dest,
                "fl_date": {"$gte": date_from, "$lte": date_to},
            }
        },
        {
            "$lookup": {
                "from": "flights_performance",
                "localField": "_id",
                "foreignField": "flight_id",
                "as": "performance",
            }
        },
        {
            "$lookup": {
                "from": "flights_delayed",
                "localField": "_id",
                "foreignField": "flight_id",
                "as": "delayed",
            }
        },
        {
            "$lookup": {
                "from": "flights_cancelled",
                "localField": "_id",
                "foreignField": "flight_id",
                "as": "cancelled",
            }
        },
        {"$limit": limit},
    ]

    t0 = time.perf_counter()
    docs = list(col_f.aggregate(pipeline, allowDiskUse=True))
    dt = (time.perf_counter() - t0) * 1000.0

    return dt, f"count={len(docs)}"


def s_mongo_rank_punctual_airlines(cfg):
    """
    Analog mysql_rank_punctual_airlines:
    - dla kolejnego miesiąca (1..12 albo lista z configu) liczy score per carrier
    - score = avg_arr_delay + (cancelled_count * cancellation_weight / total_flights) * 100
    - zwraca month=X, most_punctual=ZZ
    """
    c = mongo_client()
    db = c["flightsdb"]
    col_f = db["flights"]
    col_cancel = db["flights_cancelled"]
    col_perf = db["flights_performance"]

    rank_cfg = cfg["queries"]["airlines_ranking"]
    limit = int(rank_cfg["limit"])
    cancellation_weight = float(rank_cfg["cancellation_weight"])

    months = rank_cfg.get("months")
    if not months:
        months = list(range(1, 13))

    idx = rank_cfg.get("_next_month_idx", 0)
    rank_cfg["_next_month_idx"] = idx + 1
    month = int(months[idx % len(months)])

    t0 = time.perf_counter()

    flights = list(col_f.find({"month": month}, projection={"_id": 1, "op_unique_carrier": 1}))

    if not flights:
        dt = (time.perf_counter() - t0) * 1000.0
        return dt, f"month={month}, no_results"

    stats = {}  

    id_to_carrier = {f["_id"]: f.get("op_unique_carrier", "UNK") for f in flights}

    perf_docs = col_perf.find(
        {"flight_id": {"$in": list(id_to_carrier.keys())}},
        projection={"flight_id": 1, "arr_delay": 1},
    )

    for doc in perf_docs:
        fid = doc["flight_id"]
        carrier = id_to_carrier.get(fid, "UNK")
        arr_delay = doc.get("arr_delay")
        if carrier not in stats:
            stats[carrier] = {"sum_delay": 0.0, "total": 0, "cancelled": 0}
        if arr_delay is not None:
            stats[carrier]["sum_delay"] += float(arr_delay)
        stats[carrier]["total"] += 1

    cancel_docs = col_cancel.find(
        {"flight_id": {"$in": list(id_to_carrier.keys())}},
        projection={"flight_id": 1},
    )
    for doc in cancel_docs:
        fid = doc["flight_id"]
        carrier = id_to_carrier.get(fid, "UNK")
        if carrier not in stats:
            stats[carrier] = {"sum_delay": 0.0, "total": 0, "cancelled": 0}
        stats[carrier]["cancelled"] += 1

    scores = []
    for carrier, s in stats.items():
        total = max(s["total"], 1)
        avg_delay = s["sum_delay"] / total if s["total"] > 0 else 0.0
        cancel_component = (s["cancelled"] * cancellation_weight / total) * 100.0
        score = avg_delay + cancel_component
        scores.append((carrier, score))

    if not scores:
        dt = (time.perf_counter() - t0) * 1000.0
        return dt, f"month={month}, no_results"

    scores.sort(key=lambda x: x[1])
    top_carriers = scores[:limit]
    best = top_carriers[0][0]

    dt = (time.perf_counter() - t0) * 1000.0
    note = f"month={month}, most_punctual={best}"
    return dt, note


def s_mongo_read_by_carrier_day(cfg):
    c = mongo_client()
    col = c["flightsdb"]["flights"]
    q = cfg["queries"]["read_by_carrier_day"]
    query = {
        "op_unique_carrier": q["carrier"],
        "fl_date": {"$gte": q["date_from"], "$lte": q["date_to"]},
    }
    t0 = time.perf_counter()
    docs = list(col.find(query).limit(int(q["limit"])))
    dt = (time.perf_counter() - t0) * 1000
    return dt, f"found={len(docs)}"


def s_mongo_top_routes_month(cfg):
    c = mongo_client()
    col = c["flightsdb"]["flights"]
    month = cfg["queries"]["top_routes_month"]["month"]
    y, m = map(int, month.split("-"))
    start = f"{y}-{m:02d}-01"
    end = f"{y+1}-01-01" if m == 12 else f"{y}-{m+1:02d}-01"
    pipeline = [
        {"$match": {"fl_date": {"$gte": start, "$lt": end}}},
        {
            "$group": {
                "_id": {"origin": "$origin", "dest": "$dest"},
                "avg_arr_delay": {"$avg": "$arr_delay"},
                "cnt": {"$sum": 1},
            }
        },
        {"$sort": {"avg_arr_delay": -1}},
        {"$limit": 10},
    ]
    t0 = time.perf_counter()
    res = list(col.aggregate(pipeline, allowDiskUse=True))
    dt = (time.perf_counter() - t0) * 1000
    return dt, f"rows={len(res)}"


def s_mongo_histogram_arr_delay(cfg):
    c = mongo_client()
    col = c["flightsdb"]["flights"]
    bins = cfg["queries"]["histogram_arr_delay"]["bins"]
    pipeline = [
        {
            "$bucket": {
                "groupBy": "$arr_delay",
                "boundaries": bins,
                "default": "other",
                "output": {"count": {"$sum": 1}},
            }
        }
    ]
    t0 = time.perf_counter()
    res = list(col.aggregate(pipeline, allowDiskUse=True))
    dt = (time.perf_counter() - t0) * 1000
    return dt, f"buckets={len(res)}"


def s_mongo_insert_batch(cfg):
    c = mongo_client()
    col = c["flightsdb"]["flights"]
    n = int(cfg["crud"]["sample_size_for_writes"])
    sample = [
        {
            "fl_date": "2024-02-01",
            "op_unique_carrier": "ZZ",
            "op_carrier_fl_num": i,
            "origin": "AAA",
            "dest": "BBB",
            "arr_delay": i % 60,
            "dep_delay": i % 30,
        }
        for i in range(n)
    ]
    t0 = time.perf_counter()
    col.insert_many(sample, ordered=False)
    dt = (time.perf_counter() - t0) * 1000
    return dt, f"inserted={n}"


def s_mongo_update_many(cfg):
    c = mongo_client()
    col = c["flightsdb"]["flights"]
    t0 = time.perf_counter()
    res = col.update_many({"op_unique_carrier": "ZZ"}, {"$inc": {"arr_delay": 1}})
    dt = (time.perf_counter() - t0) * 1000
    return dt, f"matched={res.matched_count}, modified={res.modified_count}"


def s_mongo_delete_many(cfg):
    c = mongo_client()
    col = c["flightsdb"]["flights"]
    t0 = time.perf_counter()
    res = col.delete_many({"op_unique_carrier": "ZZ"})
    dt = (time.perf_counter() - t0) * 1000
    return dt, f"deleted={res.deleted_count}"


SCENARIOS_MONGO = [
    ("mongo_add_flight", s_mongo_add_flight),
    ("mongo_add_flight_stats", s_mongo_add_flight_stats),
    ("mongo_top_routes_month", s_mongo_top_routes_month),
    ("mongo_histogram_arr_delay", s_mongo_histogram_arr_delay),
    ("mongo_find_route_with_stats", s_mongo_find_route_with_stats),
    ("mongo_rank_punctual_airlines", s_mongo_rank_punctual_airlines),

    ("mongo_read_by_carrier_day", s_mongo_read_by_carrier_day),
    ("mongo_insert_batch", s_mongo_insert_batch),
    ("mongo_update_many", s_mongo_update_many),
    ("mongo_delete_many", s_mongo_delete_many),
]


def run_mongo(cfg, dataset_size: int, dataset_name: str):
    crud_cfg = cfg.setdefault("crud", {})
    crud_cfg["sample_size_for_writes"] = dataset_size
    crud_cfg.setdefault("sample_size_for_reads", dataset_size)

    for name, fn in SCENARIOS_MONGO:
        for r in range(1, int(cfg["repeats"]) + 1):
            ms, notes = fn(cfg)
            log_result("mongo", dataset_name, name, r, ms, notes)
            print(f"[mongo][{name}][run={r}] {ms:.2f} ms :: {notes}")
