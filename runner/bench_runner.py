import csv
import yaml
from pathlib import Path

from bench_cassandra import run_cassandra
from bench_mongo import run_mongo
from bench_mysql import run_mysql

RESULTS_PATH = Path("/app/results/results.csv")

def load_cfg():
    with open("bench_config.yml", "r") as f:
        return yaml.safe_load(f)

def ensure_results_header():
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not RESULTS_PATH.exists():
        with open(RESULTS_PATH, "w", newline="") as f:
            csv.writer(f).writerow(["ts","db","dataset","scenario","repeat","elapsed_ms","notes"])

if __name__ == "__main__":
    cfg = load_cfg()
    ensure_results_header()
    db = str(cfg.get("db", "mongo")).lower()
    if db == "mongo":
        run_mongo(cfg)
    elif db == "cassandra":
        run_cassandra(cfg)
    elif db == "mysql":
        run_mysql(cfg)
    else:
        raise SystemExit(f"Unknown db in bench_config.yml: {db}")
