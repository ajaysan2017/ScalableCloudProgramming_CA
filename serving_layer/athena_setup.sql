-- Serving layer: two external tables over the batch and speed views in
-- S3, plus the merge query the dashboard runs.
--
-- Run these in the Athena query editor, after pointing
-- LOCATION at your actual bucket paths.

CREATE EXTERNAL TABLE IF NOT EXISTS batch_view (
  wiki STRING,
  edit_count BIGINT,
  avg_bytes_changed DOUBLE,
  bot_edit_fraction DOUBLE
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/batch-views/wiki_baseline/';

CREATE EXTERNAL TABLE IF NOT EXISTS speed_view (
  wiki STRING,
  recent_count BIGINT,
  generated_at STRING
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/speed-views/wiki_recent/';

-- The Lambda "merge": current 5-minute edit count next to the all-time
-- average for the same wiki, ranked by current activity.
SELECT
  b.wiki,
  s.recent_count        AS last_5_min_edit_count,
  b.edit_count           AS all_time_edit_count,
  b.avg_bytes_changed    AS all_time_avg_bytes_changed,
  b.bot_edit_fraction    AS all_time_bot_fraction
FROM batch_view b
JOIN speed_view s
  ON b.wiki = s.wiki
ORDER BY last_5_min_edit_count DESC;
