# Real-Time Earthquake Analytics (Lambda Architecture)

Scalable Cloud Programming CA — USGS earthquake feed, processed through
a Lambda architecture (batch + speed) on AWS.

## Question this system answers

Which regions (10-degree lat/lon grid cells) have had the most
earthquakes in the last 5 minutes, and how does that compare to their
all-time average?

## Layout

```
common/grid.py                 shared region-bucketing logic
ingestion/historical_backfill.py   pulls bulk historical data (batch source)
ingestion/live_producer.py         polls USGS live feed -> Kinesis (speed source)
batch_layer/batch_job.py           PySpark: count + avg magnitude per region (EMR)
batch_layer/sequential_baseline.py single-threaded version, for benchmarking
speed_layer/speed_consumer.py      Kinesis consumer, 5-min sliding window per region
serving_layer/athena_setup.sql     Athena tables + the batch/speed merge query
```

## Running order

1. Backfill historical data (one-time):
   ```
   python ingestion/historical_backfill.py --start 2024-01-01 --end 2024-06-01 --out data/historical.jsonl
   aws s3 cp data/historical.jsonl s3://<bucket>/raw/historical/
   ```
2. Run the batch job on EMR:
   ```
   spark-submit batch_layer/batch_job.py \
       --input s3://<bucket>/raw/historical/*.jsonl \
       --output s3://<bucket>/batch-views/region_baseline
   ```
3. Start live ingestion:
   ```
   python ingestion/live_producer.py --stream-name earthquake-stream
   ```
4. Start the speed layer consumer:
   ```
   python speed_layer/speed_consumer.py --stream-name earthquake-stream --window-minutes 5
   ```
5. Create the Athena tables and run the merge query in `serving_layer/athena_setup.sql`.

## Benchmarking (Phase 3)

Compare `batch_layer/sequential_baseline.py` (single-threaded) against
`batch_layer/batch_job.py` run with different EMR core-node counts to
get the sequential-vs-parallel speedup numbers.
