-- Serving layer: two external tables over the batch and speed views in
-- S3, plus the merge query the dashboard runs.
--
-- Run these in the Athena query editor, after pointing
-- LOCATION at your actual bucket paths.

CREATE EXTERNAL TABLE IF NOT EXISTS batch_view (
  region STRING,
  event_count BIGINT,
  avg_magnitude DOUBLE
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/batch-views/region_baseline/';

CREATE EXTERNAL TABLE IF NOT EXISTS speed_view (
  region STRING,
  recent_count BIGINT,
  generated_at STRING
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/speed-views/region_recent/';

-- The Lambda "merge": current 5-minute count next to the all-time
-- average for the same region, ranked by current activity.
SELECT
  b.region,
  s.recent_count        AS last_5_min_count,
  b.event_count          AS all_time_count,
  b.avg_magnitude        AS all_time_avg_magnitude
FROM batch_view b
JOIN speed_view s
  ON b.region = s.region
ORDER BY last_5_min_count DESC;
