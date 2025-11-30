# runner/nosql_import/cassandra_import.py

import csv
from bench_cassandra import cass_client, _parse_date

def import_to_cassandra(file_name: str) -> None:
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

            fl_date_str = row.get("fl_date")
            if not fl_date_str:
                continue
            fl_date = _parse_date(fl_date_str)

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

            def to_int(name: str) -> int:
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
