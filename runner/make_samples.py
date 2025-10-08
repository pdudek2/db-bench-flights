import argparse, pandas as pd
from pathlib import Path
def make_samples(src, out, sizes=(10_000, 100_000, 1_000_000)):
    out = Path(out); out.mkdir(parents=True, exist_ok=True)
    frames, total = [], 0
    for chunk in pd.read_csv(src, chunksize=200_000):
        frames.append(chunk); total += len(chunk)
        if total >= max(sizes): break
    df = pd.concat(frames, ignore_index=True)
    for n in sizes:
        if len(df) >= n:
            p = out / f"flights_{n}.csv"
            df.head(n).to_csv(p, index=False)
            print(f"wrote {p} ({n} rows)")
        else:
            print(f"source has {len(df)} rows; cannot make {n}")
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    make_samples(a.src, a.out)
