# Real-Time Wikipedia Edit Analytics (Lambda Architecture)

Scalable Cloud Programming CA — Wikimedia recentchange event stream,
processed through a Lambda architecture (batch + speed) on AWS.

## Question this system answers

Which Wikipedia projects (wikis) are receiving the most edit activity
in the last 5 minutes, and how does that compare to their all-time
average edit rate?

## Why this dataset

`stream.wikimedia.org/v2/stream/recentchange` is a genuine real-time
push stream (Server-Sent Events) of every edit across Wikipedia and
sister projects — no polling, no API key, no historical-archive
pagination needed. Global edit rate is tens of events/sec, so a few
hours of continuous capture is enough to reach a multi-GB historical
dataset for the batch layer, while the same live connection feeds the
speed layer in real time.

## Layout

```
common/domain.py                  shared "wiki" key accessor
ingestion/stream_consumer.py      SSE client: archives to JSONL (batch source) + pushes to Kinesis (speed source)
batch_layer/batch_job.py          PySpark: edit count + avg bytes changed per wiki (EMR)
batch_layer/sequential_baseline.py single-threaded version, for benchmarking
speed_layer/speed_consumer.py     Kinesis consumer, 5-min sliding window per wiki
serving_layer/athena_setup.sql    Athena tables + the batch/speed merge query
```

## Running order

1. Start ingestion (do this first and let it run for a few hours to
   build up the historical dataset -- this single process feeds both layers):
   ```
   python ingestion/stream_consumer.py --archive-out data/wikimedia_archive.jsonl --stream-name wikimedia-stream
   ```
   (add `--no-kinesis` if AWS isn't set up yet and you just want to build the archive first)

2. Once you have enough archived data, upload it and run the batch job on EMR:
   ```
   aws s3 cp data/wikimedia_archive.jsonl s3://<bucket>/raw/wikimedia_archive/
   spark-submit batch_layer/batch_job.py \
       --input s3://<bucket>/raw/wikimedia_archive/*.jsonl \
       --output s3://<bucket>/batch-views/wiki_baseline
   ```

3. Run the speed layer consumer (while ingestion is still running live):
   ```
   python speed_layer/speed_consumer.py --stream-name wikimedia-stream --window-minutes 5
   ```

4. Create the Athena tables and run the merge query in `serving_layer/athena_setup.sql`.

## Benchmarking (Phase 3)

Compare `batch_layer/sequential_baseline.py` (single-threaded) against
`batch_layer/batch_job.py` run with different EMR core-node counts for
the sequential-vs-parallel speedup numbers. For ingestion-rate benchmarks,
replay a captured archive file back through Kinesis at different rates
instead of relying on the live stream's natural pace.
