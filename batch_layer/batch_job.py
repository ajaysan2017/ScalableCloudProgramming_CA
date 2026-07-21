"""
Batch layer: computes the "all-time" baseline view over the accumulated
Wikimedia edit archive -- total edit count and average bytes-changed
per wiki project (enwiki, dewiki, commonswiki, etc).

Run on EMR:

    spark-submit batch_job.py \
        --input s3://<bucket>/raw/wikimedia_archive/*.jsonl \
        --output s3://<bucket>/batch-views/wiki_baseline

Input schema (newline-delimited JSON, produced by
ingestion/stream_consumer.py):
    {"id":.., "time_ms":.., "wiki":.., "type":.., "namespace":.., "title":..,
     "user":.., "bot":.., "bytes_changed":..}

Output: one row per wiki:
    wiki | edit_count | avg_bytes_changed | bot_edit_fraction
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="glob/path to archived jsonl data")
    parser.add_argument("--output", required=True, help="output path for the batch view")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("wikimedia-batch-layer").getOrCreate()

    raw = spark.read.json(args.input)

    clean = raw.filter(F.col("wiki").isNotNull())

    baseline = (
        clean.groupBy("wiki")
        .agg(
            F.count("*").alias("edit_count"),
            F.avg("bytes_changed").alias("avg_bytes_changed"),
            F.avg(F.col("bot").cast("int")).alias("bot_edit_fraction"),
        )
        .orderBy(F.desc("edit_count"))
    )

    baseline.write.mode("overwrite").parquet(args.output)

    print(f"Wrote batch baseline view for {baseline.count()} wikis to {args.output}")
    baseline.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
