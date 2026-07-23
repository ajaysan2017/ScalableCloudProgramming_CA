"""
Single ingestion point for the whole project: consumes Wikimedia's
"recentchange" Server-Sent Events stream -- a genuine continuous
real-time push feed of every edit across Wikipedia and sister projects.

Stream docs: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams
Endpoint:    https://stream.wikimedia.org/v2/stream/recentchange

This one process does BOTH jobs at once:
  1. Archives every flattened event to a local JSONL file. Left running
     for a few hours, this file IS your batch layer's historical
     dataset -- no separate bulk-download API needed, unlike the
     earthquake feed. Typical global edit rate is tens of events/sec,
     so a few hours of capture comfortably reaches multi-GB.
  2. Pushes the same event to a Kinesis Data Stream for the speed layer
     to consume in real time.

You can disable either half independently (e.g. --no-kinesis while
you're just building up the historical archive before AWS is set up).

Usage:
    python stream_consumer.py --archive-out data/wikimedia_archive.jsonl \
        --stream-name wikimedia-stream

    # archive-only, no AWS needed yet:
    python stream_consumer.py --archive-out data/wikimedia_archive.jsonl --no-kinesis
"""

import argparse
import json
import sys
import time

import requests

try:
    import boto3
except ImportError:
    boto3 = None

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
RECONNECT_WAIT_SECONDS = 5

# Wikimedia enforces a User-Agent policy on all endpoints, including
# EventStreams: requests without a descriptive User-Agent (e.g. the
# default python-requests UA) are rejected with a 403. See:
# https://meta.wikimedia.org/wiki/User-Agent_policy
REQUEST_HEADERS = {
    "Accept": "text/event-stream",
    "User-Agent": (
        "ScalableCloudProgrammingCA/1.0 "
        "(https://github.com/ajaysan2017/ScalableCloudProgramming_CA; "
        "ajaysan2017@gmail.com) python-requests"
    ),
}


def flatten_event(event: dict) -> dict:
    length = event.get("length") or {}
    old_len, new_len = length.get("old"), length.get("new")
    bytes_changed = (new_len - old_len) if (old_len is not None and new_len is not None) else None

    return {
        "id": event.get("id"),
        "time_ms": int(event["timestamp"]) * 1000 if event.get("timestamp") else None,
        "wiki": event.get("wiki"),
        "type": event.get("type"),
        "namespace": event.get("namespace"),
        "title": event.get("title"),
        "user": event.get("user"),
        "bot": bool(event.get("bot")),
        "bytes_changed": bytes_changed,
    }


def sse_events(url: str):
    """
    Minimal Server-Sent Events parser over a streaming HTTP GET. Wikimedia
    sends standard SSE: lines prefixed "data: <json>", blank line ends
    each message. Lines starting with ":" are keepalive comments.

    Reconnects automatically on any connection drop.
    """
    while True:
        try:
            resp = requests.get(url, stream=True, timeout=60, headers=REQUEST_HEADERS)
            resp.raise_for_status()

            data_lines = []
            for raw_line in resp.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue
                line = raw_line.strip("\r")

                if line == "":
                    if data_lines:
                        payload = "\n".join(data_lines)
                        data_lines = []
                        try:
                            yield json.loads(payload)
                        except json.JSONDecodeError:
                            continue
                    continue

                if line.startswith(":"):
                    continue  # keepalive/comment
                if line.startswith("data:"):
                    data_lines.append(line[len("data:"):].strip())
                # ignore "event:", "id:", "retry:" fields -- not needed here

        except requests.RequestException as exc:
            print(f"stream connection dropped ({exc}), reconnecting in "
                  f"{RECONNECT_WAIT_SECONDS}s...", file=sys.stderr)
            time.sleep(RECONNECT_WAIT_SECONDS)


def run(archive_out: str, stream_name: str, use_kinesis: bool, region_name: str, report_every: int):
    kinesis = boto3.client("kinesis", region_name=region_name) if use_kinesis else None
    if use_kinesis and kinesis is None:
        raise SystemExit("boto3 is required for --kinesis mode (pip install boto3)")

    count = 0
    start_time = time.time()

    print(f"Connecting to {WIKIMEDIA_STREAM_URL}")
    print(f"Archiving to {archive_out}" + (f", pushing to Kinesis stream '{stream_name}'" if use_kinesis else " (Kinesis disabled)"))

    with open(archive_out, "a") as archive_file:
        for event in sse_events(WIKIMEDIA_STREAM_URL):
            record = flatten_event(event)
            if not record.get("wiki"):
                continue

            archive_file.write(json.dumps(record) + "\n")

            if use_kinesis:
                kinesis.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(record).encode("utf-8"),
                    PartitionKey=record["wiki"],
                )

            count += 1
            if count % report_every == 0:
                elapsed = time.time() - start_time
                rate = count / elapsed if elapsed > 0 else 0
                print(f"[{time.strftime('%H:%M:%S')}] {count:,} events archived "
                      f"({rate:.1f} events/sec avg)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume the Wikimedia recentchange stream.")
    parser.add_argument("--archive-out", default="data/wikimedia_archive.jsonl")
    parser.add_argument("--stream-name", default="wikimedia-stream")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--no-kinesis", action="store_true", help="archive only, skip Kinesis")
    parser.add_argument("--report-every", type=int, default=500)
    args = parser.parse_args()

    run(args.archive_out, args.stream_name, not args.no_kinesis, args.region, args.report_every)
