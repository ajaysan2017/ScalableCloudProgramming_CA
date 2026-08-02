"""
Speed layer: consumes the live Kinesis stream and maintains a 5-minute
sliding-window edit count per wiki, refreshing the "current view" every
few seconds -- the low-latency counterpart to the batch layer's
all-time baseline.

Same lightweight design as the earthquake project's speed layer: plain
boto3 + an in-memory deque, no Spark Structured Streaming dependency,
so it's fast to stand up and easy to demo live.

Usage:
    python speed_consumer.py --stream-name wikimedia-stream --window-minutes 5
"""

import argparse
import json
import time
from collections import deque
from datetime import datetime, timezone

import boto3

REFRESH_SECONDS = 10


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


def prune_window(window: deque, window_seconds: int):
    cutoff = time.time() - window_seconds
    while window and window[0][0] < cutoff:
        window.popleft()


def compute_view(window: deque):
    """
    Builds the per-wiki live stats (count + bot count) plus overall
    totals (edits in window, human vs. bot split) for the current
    window contents. Window entries are (ingest_time, wiki, is_bot).
    """
    wiki_stats = {}
    human_total = 0
    bot_total = 0

    for _, wiki, is_bot in window:
        stats = wiki_stats.setdefault(wiki, {"count": 0, "bot_count": 0})
        stats["count"] += 1
        if is_bot:
            stats["bot_count"] += 1
            bot_total += 1
        else:
            human_total += 1

    wiki_stats = dict(sorted(wiki_stats.items(), key=lambda kv: kv[1]["count"], reverse=True))
    total = human_total + bot_total
    bot_ratio = (bot_total / total * 100) if total else 0.0

    return wiki_stats, {
        "edits_in_window": total,
        "human_edits": human_total,
        "bot_edits": bot_total,
        "bot_ratio_pct": round(bot_ratio, 1),
    }

def run(stream_name: str, window_minutes: int, region_name: str, out_path: str):
    client = boto3.client("kinesis", region_name=region_name)
    iterators = get_shard_iterators(client, stream_name)
    window_seconds = window_minutes * 60
    window = deque()  # (ingest_time, wiki, is_bot)

    last_refresh = 0
    print(f"Consuming '{stream_name}' with a {window_minutes}-minute sliding window...")

    while True:
        for shard_id, shard_iter in list(iterators.items()):
            if shard_iter is None:
                continue
            resp = client.get_records(ShardIterator=shard_iter, Limit=200)
            iterators[shard_id] = resp.get("NextShardIterator")

            for rec in resp.get("Records", []):
                try:
                    payload = json.loads(rec["Data"])
                except (json.JSONDecodeError, TypeError):
                    continue
                wiki = payload.get("wiki")
                if wiki:
                    window.append((time.time(), wiki, bool(payload.get("bot"))))

        prune_window(window, window_seconds)

        if time.time() - last_refresh >= REFRESH_SECONDS:
            wiki_stats, totals = compute_view(window)
            output = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "window_minutes": window_minutes,
                "totals": totals,
                "wiki_stats": wiki_stats,
            }
            with open(out_path, "w") as f:
                json.dump(output, f, indent=2)
            print(f"[{output['generated_at']}] {totals['edits_in_window']} events in window, "
                  f"{len(wiki_stats)} active wikis, bot ratio {totals['bot_ratio_pct']}% -> wrote {out_path}")
            last_refresh = time.time()

        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sliding-window Kinesis consumer for the speed layer.")
    parser.add_argument("--stream-name", default="wikimedia-stream")
    parser.add_argument("--window-minutes", type=int, default=5)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--out", default="speed_view_latest.json")
    args = parser.parse_args()

    run(args.stream_name, args.window_minutes, args.region, args.out)
