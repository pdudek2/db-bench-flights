import csv
from pathlib import Path
from statistics import mean

RESULTS_PATH = Path("results/results.csv")


def load_results():
    rows = []
    with open(RESULTS_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["elapsed_ms"] = float(row["elapsed_ms"])
            row["repeat"] = int(row["repeat"])
            rows.append(row)
    return rows


def aggregate(results):
    agg = {}

    for r in results:
        key = (r["db"], r["dataset"], r["scenario"])
        agg.setdefault(key, []).append(r["elapsed_ms"])

    summary = []
    for (db, dataset, scenario), times in agg.items():
        summary.append(
            {
                "db": db,
                "dataset": dataset,
                "scenario": scenario,
                "n": len(times),
                "avg_ms": mean(times),
                "min_ms": min(times),
                "max_ms": max(times),
            }
        )

    summary.sort(key=lambda x: (x["db"], x["dataset"], x["scenario"]))
    return summary


def print_table(summary):
    header = f"{'DB':8} {'DATASET':8} {'SCENARIO':30} {'N':3} {'AVG[ms]':8} {'MIN':8} {'MAX':8}"
    print(header)
    print("-" * len(header))
    for row in summary:
        print(
            f"{row['db']:8} "
            f"{row['dataset']:8} "
            f"{row['scenario'][:30]:30} "
            f"{row['n']:3d} "
            f"{row['avg_ms']:8.2f} "
            f"{row['min_ms']:8.2f} "
            f"{row['max_ms']:8.2f}"
        )


if __name__ == "__main__":
    if not RESULTS_PATH.exists():
        raise SystemExit(f"Brak pliku {RESULTS_PATH}")

    results = load_results()
    summary = aggregate(results)
    print_table(summary)
