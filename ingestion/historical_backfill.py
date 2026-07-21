"""
Batch-layer data source: pulls bulk historical earthquake data from the
USGS fdsnws-event query API (NOT the rolling all_hour/all_day feeds,
which only ever hold the trailing 60 minutes / 24 hours).

Docs: https://earthquake.usgs.gov/fdsnws/event/1/

Strategy:
  - Query date range by date range (default: weekly chunks).
  - USGS caps each response at 20,000 events. If a chunk comes back
    at exactly the cap, we recursively bisect that date range until
    every sub-chunk is under the cap, so we never silently drop data.
  - Each GeoJSON "feature" is flattened into one flat JSON record per
    line (newline-delimited JSON), which both the batch Spark job and
    the speed layer can read with the exact same schema.

Usage:
    python historical_backfill.py --start 2024-01-01 --end 2024-06-01 \
        --out data/historical.jsonl

Output schema per record:
    {
      "id": str,
      "time_ms": int,          # event time, epoch milliseconds
      "mag": float | null,
      "lat": float,
      "lon": float,
      "depth_km": float | null,
      "place": str
    }
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta

import requests

USGS_QUERY_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_RESULT_CAP = 20000
REQUEST_TIMEOUT_SECONDS = 60
RETRY_ATTEMPTS = 3


def fetch_range(start: datetime, end: datetime, minmagnitude: float = 0):
    """Fetch one date range from USGS. Returns list of GeoJSON features."""
    params = {
        "format": "geojson",
        "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime": end.strftime("%Y-%m-%dT%H:%M:%S"),
        "minmagnitude": minmagnitude,
        "orderby": "time-asc",
        "limit": USGS_RESULT_CAP,
    }

    last_error = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(params=params, url=USGS_QUERY_URL, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            return resp.json().get("features", [])
        except requests.RequestException as exc:
            last_error = exc
            wait = 2 ** attempt
            print(f"  request failed ({exc}), retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {start} - {end} after {RETRY_ATTEMPTS} attempts: {last_error}")


def fetch_range_with_bisection(start: datetime, end: datetime, minmagnitude: float = 0, depth: int = 0):
    """
    Fetch a date range, automatically bisecting if the result hits the
    USGS result cap (meaning some events in this window were truncated).
    """
    features = fetch_range(start, end, minmagnitude)

    if len(features) < USGS_RESULT_CAP or (end - start) <= timedelta(hours=1):
        # under the cap, or the window is already tiny -- accept as-is
        return features

    midpoint = start + (end - start) / 2
    print(f"{'  ' * depth}window {start.date()}..{end.date()} hit the cap, bisecting at {midpoint}")
    left = fetch_range_with_bisection(start, midpoint, minmagnitude, depth + 1)
    right = fetch_range_with_bisection(midpoint, end, minmagnitude, depth + 1)
    return left + right


def flatten_feature(feature: dict) -> dict:
    props = feature.get("properties", {})
    coords = feature.get("geometry", {}).get("coordinates", [None, None, None])
    lon, lat, depth_km = (coords + [None, None, None])[:3]
    return {
        "id": feature.get("id"),
        "time_ms": props.get("time"),
        "mag": props.get("mag"),
        "lat": lat,
        "lon": lon,
        "depth_km": depth_km,
        "place": props.get("place"),
    }


def daterange_chunks(start: datetime, end: datetime, chunk_days: int):
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=chunk_days), end)
        yield cur, nxt
        cur = nxt


def main():
    parser = argparse.ArgumentParser(description="Backfill historical USGS earthquake data.")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--out", default="data/historical.jsonl")
    parser.add_argument("--chunk-days", type=int, default=7, help="initial chunk size before bisection")
    parser.add_argument("--minmagnitude", type=float, default=0)
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.strptime(args.end, "%Y-%m-%d")

    total_records = 0
    with open(args.out, "w") as f:
        for chunk_start, chunk_end in daterange_chunks(start, end, args.chunk_days):
            print(f"Fetching {chunk_start.date()} .. {chunk_end.date()}")
            features = fetch_range_with_bisection(chunk_start, chunk_end, args.minmagnitude)
            for feature in features:
                record = flatten_feature(feature)
                f.write(json.dumps(record) + "\n")
                total_records += 1
            print(f"  -> {len(features)} events (running total: {total_records})")

    print(f"Done. Wrote {total_records} records to {args.out}")
    print("Next: aws s3 cp <out> s3://<your-bucket>/raw/historical/ to stage it for the batch job.")


if __name__ == "__main__":
    main()
