import argparse
import csv
from pathlib import Path


def make_samples(src: str, out: str, sizes=(10_000, 100_000, 1_000_000)):
    
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)

    sizes = sorted(set(int(s) for s in sizes))
    remaining = {s: s for s in sizes}

    files = {}
    writers = {}
    for s in sizes:
        dst_path = out / f"flights_{s}.csv"
        f = open(dst_path, "w", newline="")
        w = csv.writer(f)
        files[s] = f
        writers[s] = w

    try:
        with open(src, newline="") as f_in:
            reader = csv.reader(f_in)
            try:
                header = next(reader)
            except StopIteration:
                print("Źródłowy plik jest pusty.")
                return

            for s in sizes:
                writers[s].writerow(header)

            active_sizes = list(sizes)

            for row in reader:
                if not active_sizes:
                    break

                for s in list(active_sizes):
                    if remaining[s] > 0:
                        writers[s].writerow(row)
                        remaining[s] -= 1

                        if remaining[s] == 0:
                            dst_path = out / f"flights_{s}.csv"
                            print(f"wrote {dst_path} ({s} rows)")
                            active_sizes.remove(s)

        for s, left in remaining.items():
            if left > 0:
                print(f"source has not enough rows for {s} (missing {left})")

    finally:
        for f in files.values():
            f.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        default=[10_000, 100_000, 1_000_000],
        help="sample sizes, e.g. --sizes 10000 1000000 7000000",
    )
    a = ap.parse_args()
    make_samples(a.src, a.out, a.sizes)
