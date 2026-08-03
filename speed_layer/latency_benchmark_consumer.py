"""
Speed layer latency benchmark -- consumer/measurement half.

Reads the Kinesis stream and, for any record tagged with the benchmark
wiki name (produced by latency_benchmark_producer.py), computes the
end-to-end latency: local receive time minus the `sent_at` timestamp
embedded by the producer. Reports throughput actually observed plus
min/mean/median/p95/max latency -- run this at several producer --rate
values to build the "latency vs ingestion rate" plot for Phase 3.

IMPORTANT: start this consumer BEFORE running the producer. It opens a
LATEST shard iterator, which only sees records written after the
iterator is created -- if the producer starts first, its early events
will be missed. Give this a couple of seconds to start up, then kick off
the producer in a separate terminal/session.

Usage:
    python latency_benchmark_consumer.py --stream-name wikimedia-stream \
        --duration 30 --out latencies_rate20.json
"""

import argparse
import json
import statistics
import time

import boto3

DEFAULT_TAG = "benchmarktestwiki"


def get_shard_iterators(client, stream_name: str):
    shards = client.describe_stream(StreamName=stream_name)["StreamDescription"]["Shards"]
    iterators = {}
    for shard in shards:
        resp = client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard["ShardId"],
            ShardIteratorType="LATEST",
        )
        iterators[shard["ShardId"]] = resp["ShardIterator"]
    return iterators


def summarize(latencies):
    latencies_sorted = sorted(latencies)
    n = len(latencies_sorted)
    p95_index = max(0, int(n * 0.95) - 1)
    return {
        "count": n,
        "min": min(latencies_sorted),
        "mean": statistics.mean(latencies_sorted),
        "median": statistics.median(latencies_sorted),
        "p95": latencies_sorted[p95_index],
        "max": max(latencies_sorted),
    }


def main():
    parser = argparse.ArgumentParser(description="Measure end-to-end Kinesis latency for tagged synthetic events.")
    parser.add_argument("--stream-name", default="wikimedia-stream")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--duration", type=float, default=30, help="how long to listen, in seconds")
    parser.add_argument("--tag", default=DEFAULT_TAG, help="synthetic wiki name to filter for (must match the producer's --tag)")
    parser.add_argument("--out", default=None, help="optional path to dump the raw latency list as JSON")
    args = parser.parse_args()

    client = boto3.client("kinesis", region_name=args.region)
    iterators = get_shard_iterators(client, args.stream_name)

    latencies = []
    start = time.time()
    end_time = start + args.duration

    print(f"Listening on '{args.stream_name}' for {args.duration}s, filtering wiki == '{args.tag}'...")
    print("(start the producer now, in another terminal)")

    while time.time() < end_time:
        for shard_id, shard_iter in list(iterators.items()):
            if shard_iter is None:
                continue
            resp = client.get_records(ShardIterator=shard_iter, Limit=500)
            iterators[shard_id] = resp.get("NextShardIterator")
            now = time.time()
            for rec in resp.get("Records", []):
                try:
                    payload = json.loads(rec["Data"])
                except (json.JSONDecodeError, TypeError):
                    continue
                if payload.get("wiki") != args.tag or "sent_at" not in payload:
                    continue
                latencies.append(now - payload["sent_at"])
        time.sleep(0.2)

    elapsed = time.time() - start

    if latencies:
        stats = summarize(latencies)
        throughput = stats["count"] / elapsed
        print(f"\nReceived {stats['count']} tagged events in {elapsed:.1f}s "
              f"(observed throughput ~{throughput:.1f} events/sec)")
        print(f"Latency (seconds): min={stats['min']:.3f}  mean={stats['mean']:.3f}  "
              f"median={stats['median']:.3f}  p95={stats['p95']:.3f}  max={stats['max']:.3f}")
    else:
        print("\nNo tagged events received -- make sure the producer ran while this "
              "was listening, and that --tag matches on both sides.")

    if args.out and latencies:
        with open(args.out, "w") as f:
            json.dump(latencies, f)
        print(f"Raw latencies written to {args.out}")


if __name__ == "__main__":
    main()