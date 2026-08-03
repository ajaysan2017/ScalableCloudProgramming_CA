"""
Speed layer latency benchmark -- producer half.

Pushes synthetic, clearly-tagged events onto the same Kinesis stream the
real ingestion pipeline uses, at a controlled target rate, each carrying
its own send timestamp (`sent_at`). Paired with
latency_benchmark_consumer.py, this measures genuine end-to-end
speed-layer latency (time from an event being written to Kinesis to it
being read back out) at a chosen ingestion rate -- this is the "latency
vs ingestion rate" benchmark the Phase 3 Performance section asks for.

The tagged wiki name (default "benchmarktestwiki") does not collide with
any real Wikipedia project, so these events are trivially distinguishable
from real traffic and safe to run against the live stream. They will
briefly appear in the dashboard's live feed as a "NEW" wiki (no batch
baseline exists for a fake name) and age out of the speed layer's window
a few minutes after the test ends -- harmless.

Usage (run the consumer FIRST in another terminal, then this):
    python latency_benchmark_producer.py --stream-name wikimedia-stream \
        --rate 20 --duration 20
"""

import argparse
import json
import time

import boto3

DEFAULT_TAG = "benchmarktestwiki"


def main():
    parser = argparse.ArgumentParser(description="Push synthetic, timestamped events at a controlled rate for speed-layer latency testing.")
    parser.add_argument("--stream-name", default="wikimedia-stream")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--rate", type=float, required=True, help="target events per second")
    parser.add_argument("--duration", type=float, default=20, help="seconds to run")
    parser.add_argument("--tag", default=DEFAULT_TAG, help="synthetic wiki name used to tag/filter these test events")
    args = parser.parse_args()

    client = boto3.client("kinesis", region_name=args.region)
    interval = 1.0 / args.rate
    end_time = time.time() + args.duration
    sent = 0

    print(f"Sending synthetic events tagged '{args.tag}' at ~{args.rate}/s for {args.duration}s...")

    next_send = time.time()
    while time.time() < end_time:
        payload = {
            "wiki": args.tag,
            "bot": False,
            "bytes_changed": 1,
            "sent_at": time.time(),
            "seq": sent,
        }
        client.put_record(
            StreamName=args.stream_name,
            Data=json.dumps(payload).encode("utf-8"),
            PartitionKey=args.tag,
        )
        sent += 1
        next_send += interval
        sleep_for = next_send - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)

    elapsed = time.time() - (end_time - args.duration)
    print(f"Done. Sent {sent} events in {elapsed:.1f}s (actual rate ~{sent / elapsed:.1f}/s)")


if __name__ == "__main__":
    main()