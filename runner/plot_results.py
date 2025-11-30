#!/usr/bin/env python
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
RESULTS_PATH = BASE_DIR / "results" / "results.csv"
CHARTS_DIR = BASE_DIR / "results" / "charts"
KNOWN_DBS = ["mysql", "postgres", "mongo", "cassandra"]
DB_COLORS = {
    "mysql": "#1f77b4",
    "postgres": "#ff7f0e",
    "mongo": "#2ca02c",
    "cassandra": "#d62728",
}


def load_results() -> pd.DataFrame:
    print(f"[plot] Szukam wyników w: {RESULTS_PATH}")
    if not RESULTS_PATH.exists():
        print(f"[plot] Nie znalazłem pliku z wynikami: {RESULTS_PATH}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(RESULTS_PATH)

    if "elapsed_ms" not in df.columns:
        raise ValueError("Brak kolumny 'elapsed_ms' w results.csv")

    df["elapsed_ms"] = df["elapsed_ms"].astype(float)

    def parse_dataset(d):
        d = str(d).strip().lower()
        if d.endswith("k"):
            return int(d[:-1]) * 1000
        if d.endswith("m"):
            return int(float(d[:-1]) * 1_000_000)
        try:
            return int(d)
        except ValueError:
            return d

    if "dataset" not in df.columns:
        raise ValueError("Brak kolumny 'dataset' w results.csv")

    df["dataset_size_num"] = df["dataset"].map(parse_dataset)

    def scenario_to_operation(s: str) -> str:
        s = str(s)
        parts = s.split("_", 1)
        if len(parts) == 2:
            return parts[1] or s
        return s

    df["operation"] = df["scenario"].map(scenario_to_operation)
    return df


def plot_scenario_lines(df: pd.DataFrame) -> None:
    print(f"[plot] Katalog na wykresy: {CHARTS_DIR}")
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    required_cols = {"operation", "db", "dataset", "dataset_size_num", "elapsed_ms"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Brak wymaganych kolumn w DataFrame: {missing}")

    grouped = (
        df.groupby(["operation", "db", "dataset", "dataset_size_num"], as_index=False)
          .agg(mean_ms=("elapsed_ms", "mean"))
    )

    operations = sorted(grouped["operation"].unique())
    if not operations:
        print("[plot] Brak operacji w danych (kolumna 'operation' pusta?).")
        return

    for op_name in operations:
        sub = grouped[grouped["operation"] == op_name].copy()
        if sub.empty:
            continue

        sub = sub.sort_values("dataset_size_num")

        # Use numeric x-values for ordering, but remember human readable labels
        dataset_labels = (
            sub[["dataset_size_num", "dataset"]]
            .drop_duplicates("dataset_size_num")
            .sort_values("dataset_size_num")
        )

        plt.figure()

        db_order = KNOWN_DBS + [
            db for db in sorted(sub["db"].unique())
            if db not in KNOWN_DBS
        ]

        for db in db_order:
            db_sub = sub[sub["db"] == db]
            if db_sub.empty:
                continue

            x = db_sub["dataset_size_num"]
            y = db_sub["mean_ms"]

            plt.plot(
                x,
                y,
                marker="o",
                label=db,
                color=DB_COLORS.get(db),
            )

        plt.xlabel("Rozmiar próbki (wiersze)")
        plt.ylabel("Średni czas [ms]")
        plt.title(f"{op_name} – średni czas vs. rozmiar danych")
        plt.xticks(
            dataset_labels["dataset_size_num"],
            dataset_labels["dataset"],
        )
        plt.legend(title="Baza danych")
        plt.grid(True, linestyle="--", alpha=0.5)

        out_path = CHARTS_DIR / f"{op_name}_by_dataset.png"
        plt.tight_layout()
        plt.savefig(out_path)
        plt.close()
        print(f"[plot] Zapisano wykres: {out_path}")


def main():
    df = load_results()
    plot_scenario_lines(df)


if __name__ == "__main__":
    main()
