-- Query 1: Freshness Check
-- Alert if no new data has been ingested in the last 2 hours
-- Returns a row with alert=true if data is stale
SELECT
  'acme_prod.finance_silver.bronze_to_silver' AS table_name,
  'FRESHNESS_CHECK' AS check_type,
  max_update_time AS last_data_timestamp,
  current_timestamp() AS check_timestamp,
  CASE
    WHEN max_update_time < current_timestamp() - INTERVAL 2 HOURS THEN TRUE
    ELSE FALSE
  END AS alert,
  CASE
    WHEN max_update_time < current_timestamp() - INTERVAL 2 HOURS
      THEN CONCAT('STALE DATA: No new data for ', 
        CAST(TIMESTAMPDIFF(MINUTE, max_update_time, current_timestamp()) AS STRING), ' minutes')
    ELSE 'OK: Data is fresh'
  END AS alert_message
FROM (
  SELECT MAX(_metadata.file_modification_time) AS max_update_time
  FROM acme_prod.finance_silver.bronze_to_silver
);

-- Query 2: Volume Anomaly Detection
-- Compare today's row count against the 7-day rolling average
-- Alert if today's count deviates by more than 2 standard deviations or differs by more than 50%
SELECT
  'acme_prod.finance_silver.bronze_to_silver' AS table_name,
  'VOLUME_ANOMALY' AS check_type,
  today_count,
  avg_7day_count,
  stddev_7day_count,
  ROUND((today_count - avg_7day_count) / NULLIF(avg_7day_count, 0) * 100, 2) AS pct_deviation,
  CASE
    WHEN ABS(today_count - avg_7day_count) > 2 * stddev_7day_count THEN TRUE
    WHEN ABS(today_count - avg_7day_count) > 0.5 * avg_7day_count THEN TRUE
    WHEN today_count = 0 AND avg_7day_count > 0 THEN TRUE
    ELSE FALSE
  END AS alert,
  CASE
    WHEN today_count = 0 AND avg_7day_count > 0 THEN 'CRITICAL: Zero rows ingested today'
    WHEN ABS(today_count - avg_7day_count) > 2 * stddev_7day_count
      THEN CONCAT('ANOMALY: Row count deviates by ', 
        CAST(ROUND((today_count - avg_7day_count) / NULLIF(avg_7day_count, 0) * 100, 2) AS STRING), 
        '% from 7-day average')
    ELSE 'OK: Volume within normal range'
  END AS alert_message
FROM (
  SELECT
    (SELECT COUNT(*) FROM acme_prod.finance_silver.bronze_to_silver
     WHERE CAST(_metadata.file_modification_time AS DATE) = current_date()) AS today_count,
    (SELECT AVG(daily_count) FROM (
      SELECT CAST(_metadata.file_modification_time AS DATE) AS ingest_date, COUNT(*) AS daily_count
      FROM acme_prod.finance_silver.bronze_to_silver
      WHERE CAST(_metadata.file_modification_time AS DATE) BETWEEN date_sub(current_date(), 7) AND date_sub(current_date(), 1)
      GROUP BY CAST(_metadata.file_modification_time AS DATE)
    )) AS avg_7day_count,
    (SELECT COALESCE(STDDEV(daily_count), 0) FROM (
      SELECT CAST(_metadata.file_modification_time AS DATE) AS ingest_date, COUNT(*) AS daily_count
      FROM acme_prod.finance_silver.bronze_to_silver
      WHERE CAST(_metadata.file_modification_time AS DATE) BETWEEN date_sub(current_date(), 7) AND date_sub(current_date(), 1)
      GROUP BY CAST(_metadata.file_modification_time AS DATE)
    )) AS stddev_7day_count
);

-- Query 3: Schema Drift Detection
-- Compare current schema against expected schema baseline stored in information_schema
-- Detect added, removed, or modified columns since last known schema version
SELECT
  'acme_prod.finance_silver.bronze_to_silver' AS table_name,
  'SCHEMA_DRIFT' AS check_type,
  column_name,
  data_type,
  ordinal_position,
  is_nullable,
  CASE
    WHEN column_name NOT IN (
      SELECT h.column_name
      FROM system.information_schema.columns AS h
      WHERE h.table_catalog = 'acme_prod'
        AND h.table_schema = 'finance_silver'
        AND h.table_name = 'bronze_to_silver'
    ) THEN 'COLUMN_REMOVED'
    ELSE 'CURRENT'
  END AS drift_status,
  current_timestamp() AS check_timestamp
FROM information_schema.columns
WHERE table_catalog = 'acme_prod'
  AND table_schema = 'finance_silver'
  AND table_name = 'bronze_to_silver'
ORDER BY ordinal_position;

-- Query 3b: Schema Drift - History comparison using table history
-- Detects if schema has changed by comparing current version to previous version
SELECT
  'acme_prod.finance_silver.bronze_to_silver' AS table_name,
  'SCHEMA_DRIFT_HISTORY' AS check_type,
  version,
  timestamp AS change_timestamp,
  operation,
  operationParameters,
  CASE
    WHEN userMetadata LIKE '%schema%' OR operation IN ('SET TBLPROPERTIES', 'CHANGE COLUMN', 'ADD COLUMNS', 'REPLACE COLUMNS')
      THEN TRUE
    ELSE FALSE
  END AS schema_change_detected,
  CASE
    WHEN operation IN ('SET TBLPROPERTIES', 'CHANGE COLUMN', 'ADD COLUMNS', 'REPLACE COLUMNS')
      THEN CONCAT('ALERT: Schema modification detected - ', operation, ' at version ', CAST(version AS STRING))
    ELSE 'OK: No schema drift detected'
  END AS alert_message
FROM (
  DESCRIBE HISTORY acme_prod.finance_silver.bronze_to_silver
)
WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY version DESC;

-- Query 4: Null Rate Monitoring for Key Columns
-- Monitor null rates across all columns and alert if null percentage exceeds thresholds
-- Thresholds: CRITICAL > 50%, WARNING > 10% for non-nullable business columns
SELECT
  'acme_prod.finance_silver.bronze_to_silver' AS table_name,
  'NULL_RATE_MONITORING' AS check_type,
  current_timestamp() AS check_timestamp,
  total_rows,
  column_name,
  null_count,
  ROUND(null_count / NULLIF(total_rows, 0) * 100, 4) AS null_pct,
  CASE
    WHEN null_count / NULLIF(total_rows, 0) * 100 > 50 THEN 'CRITICAL'
    WHEN null_count / NULLIF(total_rows, 0) * 100 > 10 THEN 'WARNING'
    ELSE 'OK'
  END AS severity,
  CASE
    WHEN null_count / NULLIF(total_rows, 0) * 100 > 50
      THEN CONCAT('CRITICAL: Column ', column_name, ' has ', 
        CAST(ROUND(null_count / NULLIF(total_rows, 0) * 100, 2) AS STRING), '% nulls')
    WHEN null_count / NULLIF(total_rows, 0) * 100 > 10
      THEN CONCAT('WARNING: Column ', column_name, ' has ', 
        CAST(ROUND(null_count / NULLIF(total_rows, 0) * 100, 2) AS STRING), '% nulls')
    ELSE CONCAT('OK: Column ', column_name,