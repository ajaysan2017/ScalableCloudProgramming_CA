"""
Batch layer: computes the "all-time" baseline view over the full
historical dataset -- total event count and average magnitude per
10-degree grid region.

Run on EMR (or locally against a sample file for testing):

    spark-submit batch_job.py \
        --input s3://<bucket>/raw/historical/*.jsonl \
        --output s3://<bucket>/batch-views/region_baseline

Input schema (newline-delimited JSON, one record per line -- produced by
ingestion/historical_backfill.py):
    {"id": ..., "time_ms": ..., "mag": ..., "lat": ..., "lon": ..., "depth_km": ..., "place": ...}

Output: a Parquet dataset partitioned by nothing in particular, one row
per region:
    region | event_count | avg_magnitude
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


def build_region_column(df):
    """
    Bucket each row into a 10-degree lat/lon grid region, matching
    common/grid.py's grid_key() logic exactly (kept as a separate,
    Spark-native implementation since UDFs calling into shared Python
    modules add packaging overhead on EMR -- the formula itself must
    stay identical to the speed layer's).
    """
    grid_size = F.lit(10)
    lat_bucket = F.floor(F.col("lat") / grid_size) * grid_size
    lon_bucket = F.floor(F.col("lon") / grid_size) * grid_size
    return df.withColumn(
        "region",
        F.concat_ws("_", lat_bucket.cast(IntegerType()), lon_bucket.cast(IntegerType())),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="glob/path to historical jsonl data")
    parser.add_argument("--output", required=True, help="output path for the batch view")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("earthquake-batch-layer").getOrCreate()

    raw = spark.read.json(args.input)

    clean = (
        raw.withColumn("mag", F.col("mag").cast(DoubleType()))
        .withColumn("lat", F.col("lat").cast(DoubleType()))
        .withColumn("lon", F.col("lon").cast(DoubleType()))
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
    )

    regioned = build_region_column(clean)

    baseline = (
        regioned.groupBy("region")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("mag").alias("avg_magnitude"),
        )
        .orderBy(F.desc("event_count"))
    )

    baseline.write.mode("overwrite").parquet(args.output)

    print(f"Wrote batch baseline view for {baseline.count()} regions to {args.output}")
    baseline.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
