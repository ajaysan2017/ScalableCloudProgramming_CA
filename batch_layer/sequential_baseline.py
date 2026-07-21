"""
Pure-Python, single-threaded version of the same aggregation the Spark
batch job does -- the sequential baseline for Phase 3's "sequential vs
parallel" benchmark.

Usage:
    time python sequential_baseline.py --input data/wikimedia_archive.jsonl
"""

import argparse
import json
import time
from collections import defaultdict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    counts = defaultdict(int)
    bytes_sums = defaultdict(float)
    bytes_counts = defaultdict(int)

    start = time.time()

    with open(args.input) as f:
        for line in f:
            record = json.loads(line)
            wiki = record.get("wiki")
            if not wiki:
                continue
            counts[wiki] += 1
            bc = record.get("bytes_changed")
            if bc is not None:
                bytes_sums[wiki] += bc
                bytes_counts[wiki] += 1

    elapsed = time.time() - start

    results = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    print(f"Processed {sum(counts.values())} records across {len(counts)} wikis in {elapsed:.2f}s (sequential)")
    print("wiki, edit_count, avg_bytes_changed")
    for wiki, count in results[:20]:
        avg_bc = bytes_sums[wiki] / bytes_counts[wiki] if bytes_counts[wiki] else None
        print(f"{wiki}, {count}, {avg_bc}")


if __name__ == "__main__":
    main()
