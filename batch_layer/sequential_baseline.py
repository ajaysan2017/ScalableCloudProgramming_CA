"""
Pure-Python, single-threaded version of the same aggregation the Spark
batch job does. This exists purely so Phase 3's benchmark section has a
genuine sequential baseline to compare Spark's parallel execution
against (the rubric explicitly asks for "sequential vs parallel
execution of the batch job").

Usage:
    time python sequential_baseline.py --input data/historical.jsonl
"""

import argparse
import json
import math
import time
from collections import defaultdict


def grid_key(lat: float, lon: float, size: int = 10) -> str:
    lat_bucket = int(math.floor(lat / size) * size)
    lon_bucket = int(math.floor(lon / size) * size)
    return f"{lat_bucket}_{lon_bucket}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    counts = defaultdict(int)
    mag_sums = defaultdict(float)
    mag_counts = defaultdict(int)

    start = time.time()

    with open(args.input) as f:
        for line in f:
            record = json.loads(line)
            lat, lon, mag = record.get("lat"), record.get("lon"), record.get("mag")
            if lat is None or lon is None:
                continue
            region = grid_key(lat, lon)
            counts[region] += 1
            if mag is not None:
                mag_sums[region] += mag
                mag_counts[region] += 1

    elapsed = time.time() - start

    results = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    print(f"Processed {sum(counts.values())} records across {len(counts)} regions in {elapsed:.2f}s (sequential)")
    print("region, event_count, avg_magnitude")
    for region, count in results[:20]:
        avg_mag = mag_sums[region] / mag_counts[region] if mag_counts[region] else None
        print(f"{region}, {count}, {avg_mag}")


if __name__ == "__main__":
    main()
