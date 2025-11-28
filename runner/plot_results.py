#!/usr/bin/env python
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
RESULTS_PATH = BASE_DIR / "results" / "results.csv"
CHARTS_DIR = BASE_DIR / "results" / "charts"


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
    return df


def plot_scenario_lines(df: pd.DataFrame) -> None:
    print(f"[plot] Katalog na wykresy: {CHARTS_DIR}")
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    required_cols = {"scenario", "db", "dataset", "dataset_size_num", "elapsed_ms"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Brak wymaganych kolumn w DataFrame: {missing}")

    grouped = (
        df.groupby(["scenario", "db", "dataset", "dataset_size_num"], as_index=False)
          .agg(mean_ms=("elapsed_ms", "mean"))
    )

    scenarios = sorted(grouped["scenario"].unique())
    if not scenarios:
        print("[plot] Brak scenariuszy w danych (kolumna 'scenario' pusta?).")
        return

    for scen in scenarios:
        sub = grouped[grouped["scenario"] == scen].copy()
        if sub.empty:
            continue

        sub = sub.sort_values("dataset_size_num")

        plt.figure()

        for db in sorted(sub["db"].unique()):
            db_sub = sub[sub["db"] == db]
            x = db_sub["dataset"]
            y = db_sub["mean_ms"]

            plt.plot(
                x,
                y,
                marker="o",
                label=db,
            )

        plt.xlabel("Dataset")
        plt.ylabel("Średni czas [ms]")
        plt.title(f"Czas vs rozmiar danych – {scen}")
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.5)

        out_path = CHARTS_DIR / f"{scen}_by_dataset.png"
        plt.tight_layout()
        plt.savefig(out_path)
        plt.close()
        print(f"[plot] Zapisano wykres: {out_path}")


def main():
    df = load_results()
    plot_scenario_lines(df)


if __name__ == "__main__":
    main()
