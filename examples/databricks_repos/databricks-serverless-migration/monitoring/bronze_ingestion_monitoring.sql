-- Query 1: Freshness Check
-- Alert if no new data has been ingested in the last 2 hours
-- Returns a row with is_stale = true if data is not fresh
SELECT
  'acme_prod.finance_bronze.bronze_ingestion' AS table_name,
  MAX(ingestion_timestamp) AS last_ingestion_time,
  CURRENT_TIMESTAMP() AS check_time,
  TIMESTAMPDIFF(MINUTE, MAX(ingestion_timestamp), CURRENT_TIMESTAMP()) AS minutes_since_last_ingestion,
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, MAX(ingestion_timestamp), CURRENT_TIMESTAMP()) > 120 THEN true
    ELSE false
  END AS is_stale,
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, MAX(ingestion_timestamp), CURRENT_TIMESTAMP()) > 120
    THEN CONCAT('ALERT: No new data in ', TIMESTAMPDIFF(MINUTE, MAX(ingestion_timestamp), CURRENT_TIMESTAMP()), ' minutes (threshold: 120 minutes)')
    ELSE 'OK: Data is fresh'
  END AS status_message
FROM acme_prod.finance_bronze.bronze_ingestion;

-- Query 2: Volume Anomaly Detection
-- Compares today's row count against the 7-day rolling average
-- Flags anomaly if today's count deviates more than 2 standard deviations from the average
WITH daily_counts AS (
  SELECT
    DATE(ingestion_timestamp) AS ingestion_date,
    COUNT(*) AS row_count
  FROM acme_prod.finance_bronze.bronze_ingestion
  WHERE DATE(ingestion_timestamp) >= DATE_ADD(CURRENT_DATE(), -8)
  GROUP BY DATE(ingestion_timestamp)
),
seven_day_stats AS (
  SELECT
    AVG(row_count) AS avg_row_count,
    STDDEV(row_count) AS stddev_row_count
  FROM daily_counts
  WHERE ingestion_date >= DATE_ADD(CURRENT_DATE(), -7)
    AND ingestion_date < CURRENT_DATE()
),
today_count AS (
  SELECT
    COUNT(*) AS todays_row_count
  FROM acme_prod.finance_bronze.bronze_ingestion
  WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
)
SELECT
  'acme_prod.finance_bronze.bronze_ingestion' AS table_name,
  CURRENT_DATE() AS check_date,
  t.todays_row_count,
  ROUND(s.avg_row_count, 2) AS seven_day_avg,
  ROUND(s.stddev_row_count, 2) AS seven_day_stddev,
  ROUND(((t.todays_row_count - s.avg_row_count) / NULLIF(s.stddev_row_count, 0)), 2) AS z_score,
  ROUND((t.todays_row_count - s.avg_row_count) / NULLIF(s.avg_row_count, 0) * 100, 2) AS pct_deviation,
  CASE
    WHEN s.avg_row_count = 0 AND t.todays_row_count = 0 THEN 'WARNING: No data today or in past 7 days'
    WHEN s.stddev_row_count = 0 THEN 'INFO: Zero variance in 7-day history'
    WHEN ABS((t.todays_row_count - s.avg_row_count) / s.stddev_row_count) > 2 THEN 'ALERT: Volume anomaly detected (>2 std deviations)'
    ELSE 'OK: Volume within normal range'
  END AS status_message
FROM today_count t
CROSS JOIN seven_day_stats s;

-- Query 3: Schema Drift Detection
-- Compares current table schema against a baseline stored in information_schema
-- Detects added, removed, or modified columns since last known schema snapshot
WITH current_schema AS (
  SELECT
    column_name,
    data_type,
    ordinal_position,
    is_nullable,
    column_default
  FROM acme_prod.information_schema.columns
  WHERE table_catalog = 'acme_prod'
    AND table_schema = 'finance_bronze'
    AND table_name = 'bronze_ingestion'
),
baseline_schema AS (
  SELECT
    column_name,
    data_type,
    ordinal_position,
    is_nullable,
    column_default
  FROM acme_prod.finance_bronze.bronze_ingestion_schema_baseline
),
added_columns AS (
  SELECT
    c.column_name,
    c.data_type,
    'ADDED' AS drift_type,
    CONCAT('New column detected: ', c.column_name, ' (', c.data_type, ')') AS description
  FROM current_schema c
  LEFT JOIN baseline_schema b ON c.column_name = b.column_name
  WHERE b.column_name IS NULL
),
removed_columns AS (
  SELECT
    b.column_name,
    b.data_type,
    'REMOVED' AS drift_type,
    CONCAT('Column removed: ', b.column_name, ' (', b.data_type, ')') AS description
  FROM baseline_schema b
  LEFT JOIN current_schema c ON b.column_name = c.column_name
  WHERE c.column_name IS NULL
),
modified_columns AS (
  SELECT
    c.column_name,
    c.data_type,
    'MODIFIED' AS drift_type,
    CONCAT('Column modified: ', c.column_name, ' type changed from ', b.data_type, ' to ', c.data_type,
           CASE WHEN b.is_nullable != c.is_nullable THEN CONCAT(', nullable changed from ', b.is_nullable, ' to ', c.is_nullable) ELSE '' END) AS description
  FROM current_schema c
  INNER JOIN baseline_schema b ON c.column_name = b.column_name
  WHERE c.data_type != b.data_type
    OR c.is_nullable != b.is_nullable
)
SELECT
  'acme_prod.finance_bronze.bronze_ingestion' AS table_name,
  CURRENT_TIMESTAMP() AS check_time,
  drift_type,
  column_name,
  data_type,
  description,
  CASE
    WHEN drift_type IN ('ADDED', 'REMOVED', 'MODIFIED') THEN 'ALERT: Schema drift detected'
  END AS status_message
FROM added_columns
UNION ALL
SELECT
  'acme_prod.finance_bronze.bronze_ingestion' AS table_name,
  CURRENT_TIMESTAMP() AS check_time,
  drift_type,
  column_name,
  data_type,
  description,
  'ALERT: Schema drift detected' AS status_message
FROM removed_columns
UNION ALL
SELECT
  'acme_prod.finance_bronze.bronze_ingestion' AS table_name,
  CURRENT_TIMESTAMP() AS check_time,
  drift_type,
  column_name,
  data_type,
  description,
  'ALERT: Schema drift detected' AS status_message
FROM modified_columns;

-- Query 4: Null Rate Monitoring for Key Columns
-- Monitors null percentages for critical columns and alerts if thresholds are exceeded
-- Thresholds: transaction_id (0%), account_id (0%), amount (1%), currency_code (1%), ingestion_timestamp (0%)
WITH null_stats AS (
  SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS transaction_id_nulls,
    SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS account_id_nulls,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amount_nulls,
    SUM(CASE WHEN currency_code IS NULL THEN 1 ELSE 0 END) AS currency_code_nulls,
    SUM(CASE WHEN ingestion_timestamp IS NULL THEN 1 ELSE