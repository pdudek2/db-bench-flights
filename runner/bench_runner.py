import csv
import yaml
from pathlib import Path

from bench_cassandra import run_cassandra
from bench_mongo import run_mongo
from bench_mysql import run_mysql
from make_samples import make_samples
from bench_postgres import run_postgres

RESULTS_PATH = Path("/app/results/results.csv")

def load_cfg():
    with open("bench_config.yml", "r") as f:
        return yaml.safe_load(f)

def ensure_results_header():
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not RESULTS_PATH.exists():
        with open(RESULTS_PATH, "w", newline="") as f:
            csv.writer(f).writerow(["ts", "db", "dataset", "scenario", "repeat", "elapsed_ms", "notes"])

db_runners = {
    "mongo": run_mongo,
    "mysql": run_mysql,
    "postgres": run_postgres,
    "cassandra": run_cassandra
}

def prepare_samples(cfg):
    datasets = [int(d["size"]) for d in cfg["datasets"]]
    src_file = cfg["samples"]["src_file"]
    dst_dir = cfg["samples"]["dst_dir"]
    make_samples(src_file, dst_dir, datasets)

if __name__ == "__main__":
    cfg = load_cfg()
    prepare_samples(cfg)
    ensure_results_header()
    dbs_to_run = cfg["db"]
    datasets = cfg["datasets"]

    for db in dbs_to_run:
        for dataset in datasets:
            dataset_size = int(dataset["size"])
            dataset_name = dataset["name"]
            run_function = db_runners[db]
            print(f"\nStarting tests for **{db}**, dataset size **{dataset_name}**...")
            run_function(cfg, dataset_size, dataset_name)