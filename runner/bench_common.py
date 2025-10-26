import csv
from datetime import datetime
from pathlib import Path

RESULTS_PATH = Path("/app/results/results.csv")

def log_result(db, dataset, scenario, repeat, ms, notes=""):
    with open(RESULTS_PATH, "a", newline="") as f:
        csv.writer(f).writerow([datetime.utcnow().isoformat(), db, dataset, scenario, repeat, round(ms,2), notes])