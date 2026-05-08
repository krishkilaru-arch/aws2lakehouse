-- Query 1: Freshness Check
-- Alert if no new data has been written to the table in the last 2 hours
-- Uses the table's last modification timestamp from information_schema
SELECT
    'acme_prod.finance_silver.silver_transformation' AS table_name,
    last_altered,
    TIMESTAMPDIFF(MINUTE, last_altered, CURRENT_TIMESTAMP()) AS minutes_since_update,
    CASE
        WHEN TIMESTAMPDIFF(MINUTE, last_altered, CURRENT_TIMESTAMP()) > 120
        THEN 'ALERT: Table has not been updated in over 2 hours'
        ELSE 'OK: Table is fresh'
    END AS freshness_status
FROM system.information_schema.tables
WHERE table_catalog = 'acme_prod'
    AND table_schema = 'finance_silver'
    AND table_name = 'silver_transformation';

-- Query 2: Volume Anomaly Detection
-- Compare today's row count against the 7-day rolling average
-- Flag anomalies where today's count deviates more than 2 standard deviations
WITH daily_counts AS (
    SELECT
        DATE(processing_timestamp) AS record_date,
        COUNT(*) AS daily_row_count
    FROM acme_prod.finance_silver.silver_transformation
    WHERE DATE(processing_timestamp) >= DATE_ADD(CURRENT_DATE(), -7)
    GROUP BY DATE(processing_timestamp)
),
stats AS (
    SELECT
        AVG(daily_row_count) AS avg_7day_count,
        STDDEV(daily_row_count) AS stddev_7day_count
    FROM daily_counts
    WHERE record_date < CURRENT_DATE()
),
today_count AS (
    SELECT COUNT(*) AS todays_row_count
    FROM acme_prod.finance_silver.silver_transformation
    WHERE DATE(processing_timestamp) = CURRENT_DATE()
)
SELECT
    t.todays_row_count,
    s.avg_7day_count,
    s.stddev_7day_count,
    ROUND((t.todays_row_count - s.avg_7day_count) / NULLIF(s.stddev_7day_count, 0), 2) AS z_score,
    CASE
        WHEN t.todays_row_count = 0
        THEN 'ALERT: No data ingested today'
        WHEN ABS((t.todays_row_count - s.avg_7day_count) / NULLIF(s.stddev_7day_count, 0)) > 2
        THEN 'ALERT: Volume anomaly detected (>2 std deviations from 7-day average)'
        WHEN t.todays_row_count < s.avg_7day_count * 0.5
        THEN 'WARNING: Today''s volume is less than 50% of 7-day average'
        ELSE 'OK: Volume within normal range'
    END AS volume_status
FROM today_count t
CROSS JOIN stats s;

-- Query 3: Schema Drift Detection
-- Compare current schema against a baseline stored in a governance tracking table
-- Detect added, removed, or modified columns
WITH current_schema AS (
    SELECT
        column_name,
        data_type,
        ordinal_position,
        is_nullable
    FROM system.information_schema.columns
    WHERE table_catalog = 'acme_prod'
        AND table_schema = 'finance_silver'
        AND table_name = 'silver_transformation'
),
baseline_schema AS (
    SELECT
        column_name,
        data_type,
        ordinal_position,
        is_nullable
    FROM acme_prod.finance_silver.schema_baseline
    WHERE table_name = 'silver_transformation'
        AND baseline_date = (
            SELECT MAX(baseline_date)
            FROM acme_prod.finance_silver.schema_baseline
            WHERE table_name = 'silver_transformation'
        )
)
SELECT
    COALESCE(c.column_name, b.column_name) AS column_name,
    b.data_type AS baseline_data_type,
    c.data_type AS current_data_type,
    CASE
        WHEN b.column_name IS NULL THEN 'DRIFT: New column added'
        WHEN c.column_name IS NULL THEN 'DRIFT: Column removed'
        WHEN b.data_type != c.data_type THEN 'DRIFT: Data type changed'
        WHEN b.is_nullable != c.is_nullable THEN 'DRIFT: Nullability changed'
        ELSE 'OK: No change'
    END AS drift_status,
    CURRENT_TIMESTAMP() AS checked_at
FROM current_schema c
FULL OUTER JOIN baseline_schema b
    ON c.column_name = b.column_name
WHERE b.column_name IS NULL
    OR c.column_name IS NULL
    OR b.data_type != c.data_type
    OR b.is_nullable != c.is_nullable;

-- Query 4: Null Rate Monitoring for Key Columns
-- Monitor null percentages for critical financial columns
-- Alert if null rates exceed acceptable thresholds
SELECT
    'acme_prod.finance_silver.silver_transformation' AS table_name,
    COUNT(*) AS total_rows,
    -- Key column null rates
    ROUND(SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS transaction_id_null_pct,
    ROUND(SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS account_id_null_pct,
    ROUND(SUM(CASE WHEN transaction_amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS transaction_amount_null_pct,
    ROUND(SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS transaction_date_null_pct,
    ROUND(SUM(CASE WHEN currency_code IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS currency_code_null_pct,
    ROUND(SUM(CASE WHEN counterparty_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS counterparty_id_null_pct,
    -- Overall status
    CASE
        WHEN SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'CRITICAL: transaction_id has NULL values (primary key column)'
        WHEN SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 1.0
        THEN 'ALERT: account_id null rate exceeds 1% threshold'
        WHEN SUM(CASE WHEN transaction_amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 0.5
        THEN 'ALERT: transaction_amount null rate exceeds 0.5% threshold'
        WHEN SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 0.1
        THEN 'ALERT: transaction_date null rate exceeds 0.1% threshold'
        ELSE 'OK: All null rates within acceptable thresholds'
    END AS null_monitoring_status,
    CURRENT_TIMESTAMP() AS monitored_at
FROM acme_prod.finance_silver.silver_transformation
WHERE DATE(processing_timestamp) = CURRENT_DATE();