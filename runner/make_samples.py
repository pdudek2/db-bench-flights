import argparse
import csv
import random
from pathlib import Path
from typing import Iterable, List, Sequence

def _reservoir_sample(rows: Iterable[List[str]], sample_size: int, rng: random.Random):
    """Keep an unbiased random sample limited to sample_size rows while streaming rows."""
    sample: List[List[str]] = []
    total_rows = 0

    for row in rows:
        total_rows += 1
        if len(sample) < sample_size:
            sample.append(row)
            continue

        idx = rng.randint(0, total_rows - 1)
        if idx < sample_size:
            sample[idx] = row

    return sample, total_rows

def make_samples(src: str, out: str, sizes: Sequence[int] = (10_000, 100_000, 1_000_000)):
    out_path = Path(out)
    out_path.mkdir(parents=True, exist_ok=True)

    cleaned_sizes = sorted({int(s) for s in sizes if int(s) > 0})
    if not cleaned_sizes:
        print("no valid sample sizes provided")
        return

    max_size = cleaned_sizes[-1]
    rng = random.Random()

    with open(src, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            print(f"{src} is empty; nothing to sample")
            return

        sample, total_rows = _reservoir_sample(reader, max_size, rng)

    if total_rows < max_size:
        max_size = total_rows

    rng.shuffle(sample)

    for size in cleaned_sizes:
        if total_rows < size:
            print(f"source has {total_rows} rows; cannot make {size}")
            continue

        dest = out_path / f"flights_{size}.csv"
        with open(dest, "w", newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            writer.writerow(header)
            writer.writerows(sample[:size])
        print(f"wrote {dest} ({size} rows)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--sizes", nargs="*", type=int, default=None, help="Override dataset sizes")
    args = parser.parse_args()

    sizes = tuple(args.sizes) if args.sizes else (10_000, 100_000, 1_000_000)
    make_samples(args.src, args.out, sizes)
