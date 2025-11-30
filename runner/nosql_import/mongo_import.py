# runner/nosql_import/mongo_import.py

import csv
from bench_mongo import mongo_client

def import_to_mongo(file_name: str) -> None:
    print(f"\n[IMPORTING] Importing {file_name} into MongoDB...")

    c = mongo_client()
    db = c["flightsdb"]
    col = db["flights"]

    # na wszelki wypadek czyścimy kolekcję
    col.delete_many({})

    batch = []
    batch_size = 10_000

    with open(file_name, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # lekkie konwersje na liczby – używane w agregacjach
            for key in ["arr_delay", "dep_delay", "distance"]:
                val = row.get(key)
                if val is not None and val != "":
                    try:
                        row[key] = float(val)
                    except ValueError:
                        row[key] = None

            batch.append(row)

            if len(batch) >= batch_size:
                col.insert_many(batch)
                batch.clear()

        if batch:
            col.insert_many(batch)

    print("[IMPORTING][mongo] done.")