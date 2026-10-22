import argparse
import csv
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, default=Path("data/benchmark.csv"))
parser.add_argument("--mb", type=int, default=10)
args = parser.parse_args()
args.output.parent.mkdir(parents=True, exist_ok=True)
with args.output.open("w", encoding="utf-8", newline="") as stream:
    writer = csv.writer(stream)
    writer.writerow(["order_id", "region", "amount"])
    number = 0
    while stream.tell() < args.mb * 1024 * 1024:
        writer.writerow([f"A{number % 100000:08d}", f"region-{number % 10}", "123.45"])
        number += 1
print(f"{args.output}: {number} rows, {args.output.stat().st_size} bytes")
