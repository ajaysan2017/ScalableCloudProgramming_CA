"""
Exports the batch layer's baseline (edit count + avg bytes changed per
wiki) as a plain JSON file, for the local dashboard to read directly.

This is separate from batch_job.py's Parquet output (which is what
Athena reads as the "official" AWS serving layer) -- this script gives
the dashboard a fast, AWS-independent copy of the same numbers, so the
dashboard can run and be demoed without needing a live Athena query for
every refresh.

Run this once after (or instead of, for local dev) the Spark batch job,
pointed at the same archive file:

    python export_baseline_json.py --input data/wikimedia_archive.jsonl \
        --out serving_layer/batch_baseline.json
"""

import argparse
import json
from collections import defaultdict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", default="serving_layer/batch_baseline.json")
    args = parser.parse_args()

    counts = defaultdict(int)
    bytes_sums = defaultdict(float)
    bytes_counts = defaultdict(int)
    bot_counts = defaultdict(int)

    with open(args.input) as f:
        for line in f:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            wiki = record.get("wiki")
            if not wiki:
                continue
            counts[wiki] += 1
            bc = record.get("bytes_changed")
            if bc is not None:
                bytes_sums[wiki] += bc
                bytes_counts[wiki] += 1
            if record.get("bot"):
                bot_counts[wiki] += 1

    baseline = {}
    for wiki, count in counts.items():
        avg_bc = bytes_sums[wiki] / bytes_counts[wiki] if bytes_counts[wiki] else None
        bot_fraction = bot_counts[wiki] / count if count else 0.0
        baseline[wiki] = {
            "edit_count": count,
            "avg_bytes_changed": avg_bc,
            "bot_edit_fraction": round(bot_fraction, 4),
        }
    with open(args.out, "w") as f:
        json.dump(baseline, f, indent=2)

    print(f"Wrote baseline for {len(baseline)} wikis to {args.out}")


if __name__ == "__main__":
    main()