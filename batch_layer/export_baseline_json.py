"""
Exports the batch layer's baseline (edit count, avg bytes changed, and
expected edit rate per wiki) as a plain JSON file, for the local
dashboard to read directly.

This is separate from batch_job.py's Parquet output (which is what
Athena reads as the "official" AWS serving layer) -- this script gives
the dashboard a fast, AWS-independent copy of the same numbers, so the
dashboard can run and be demoed without needing a live Athena query for
every refresh.

In addition to the per-wiki totals, this also records the archive's
overall time span (earliest to latest event timestamp seen). That span
is what lets the dashboard convert "total edits over the whole archive"
into "expected edits per 5-minute window" -- the baseline rate the
speed layer's live count gets compared against to compute a deviation
score. This is the actual analytical feature of the project: not just
showing two raw numbers side by side, but detecting when a wiki's
current activity meaningfully departs from its historical norm.

Run this once after (or instead of, for local dev) the Spark batch job,
pointed at the same archive file:

    python export_baseline_json.py --input data/wikimedia_archive.jsonl \
        --out serving_layer/batch_baseline.json
"""

import argparse
import json
import time
from collections import defaultdict

# Sanity bounds for event timestamps, used to reject corrupt/outlier
# records before they get used to compute the archive's time span.
# A single bad timestamp (e.g. from a malformed stream event) can
# otherwise blow up min()/max() to a value years off, which silently
# wrecks the "expected edits per window" calculation for every wiki
# (division by a near-zero span produces absurd deviation multipliers).
MIN_VALID_TIME_MS = 1577836800000  # 2020-01-01, well before this project
MAX_VALID_TIME_MS = int(time.time() * 1000) + 86400000  # now + 1 day buffer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", default="serving_layer/batch_baseline.json")
    args = parser.parse_args()

    counts = defaultdict(int)
    bytes_sums = defaultdict(float)
    bytes_counts = defaultdict(int)
    bot_counts = defaultdict(int)
    min_time_ms = None
    max_time_ms = None

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

            t = record.get("time_ms")
            if t is not None and MIN_VALID_TIME_MS <= t <= MAX_VALID_TIME_MS:
                if min_time_ms is None or t < min_time_ms:
                    min_time_ms = t
                if max_time_ms is None or t > max_time_ms:
                    max_time_ms = t

    span_minutes = ((max_time_ms - min_time_ms) / 60000) if (min_time_ms and max_time_ms) else None

    wikis = {}
    for wiki, count in counts.items():
        avg_bc = bytes_sums[wiki] / bytes_counts[wiki] if bytes_counts[wiki] else None
        bot_fraction = bot_counts[wiki] / count if count else 0.0
        wikis[wiki] = {
            "edit_count": count,
            "avg_bytes_changed": avg_bc,
            "bot_edit_fraction": round(bot_fraction, 4),
        }

    output = {
        "meta": {
            "span_minutes": span_minutes,
            "record_count": sum(counts.values()),
        },
        "wikis": wikis,
    }

    with open(args.out, "w") as f:
        json.dump(output, f, indent=2)

    span_desc = f"{span_minutes:.1f} min" if span_minutes else "unknown span"
    print(f"Wrote baseline for {len(wikis)} wikis ({span_desc}) to {args.out}")


if __name__ == "__main__":
    main()