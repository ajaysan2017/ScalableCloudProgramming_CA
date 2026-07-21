"""
Speed-layer ingestion: polls the USGS rolling "all_hour" feed every 60
seconds (matching USGS's own refresh cadence) and pushes any new events
into a Kinesis Data Stream, one record per event.

Each record's partition key is its grid region (see common/grid.py), so
that events for the same region tend to land on the same shard -- handy
if you want ordered per-region processing downstream.

Usage:
    python live_producer.py --stream-name earthquake-stream --poll-seconds 60

Requires AWS credentials configured (e.g. via the AWS Academy Learner
Lab's exported credentials) and the target Kinesis stream already created:

    aws kinesis create-stream --stream-name earthquake-stream --shard-count 1
"""

import argparse
import json
import sys
import time

import boto3
import requests

sys.path.append("..")
from common.grid import grid_key  # noqa: E402

USGS_ALL_HOUR_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"


def fetch_current_events():
    resp = requests.get(USGS_ALL_HOUR_URL, timeout=30)
    resp.raise_for_status()
    return resp.json().get("features", [])


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


def run(stream_name: str, poll_seconds: int, region_name: str):
    kinesis = boto3.client("kinesis", region_name=region_name)
    seen_ids = set()

    print(f"Polling {USGS_ALL_HOUR_URL} every {poll_seconds}s -> Kinesis stream '{stream_name}'")

    while True:
        try:
            features = fetch_current_events()
        except requests.RequestException as exc:
            print(f"fetch failed: {exc}", file=sys.stderr)
            time.sleep(poll_seconds)
            continue

        new_count = 0
        for feature in features:
            record = flatten_feature(feature)
            if record["id"] in seen_ids or record["lat"] is None or record["lon"] is None:
                continue
            seen_ids.add(record["id"])

            partition_key = grid_key(record["lat"], record["lon"])
            kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(record).encode("utf-8"),
                PartitionKey=partition_key,
            )
            new_count += 1

        print(f"[{time.strftime('%H:%M:%S')}] {new_count} new event(s) sent "
              f"({len(seen_ids)} total seen this run)")

        # keep the seen-id set from growing unbounded over a long-running process
        if len(seen_ids) > 5000:
            seen_ids = set(list(seen_ids)[-2000:])

        time.sleep(poll_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Poll USGS and push new events into Kinesis.")
    parser.add_argument("--stream-name", default="earthquake-stream")
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    run(args.stream_name, args.poll_seconds, args.region)
