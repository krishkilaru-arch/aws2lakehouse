-- Query 1: Freshness Check
-- Alert if no new data has been written to the table in the last 2 hours
-- Uses the table's last modified timestamp from information_schema
SELECT
    'acme_prod.finance_gold.silver_to_gold' AS table_name,
    last_altered,
    TIMESTAMPDIFF(MINUTE, last_altered, CURRENT_TIMESTAMP()) AS minutes_since_update,
    CASE
        WHEN TIMESTAMPDIFF(MINUTE, last_altered, CURRENT_TIMESTAMP()) > 120
        THEN 'ALERT: Table stale - no updates in over 2 hours'
        ELSE 'OK: Table is fresh'
    END AS freshness_status
FROM system.information_schema.tables
WHERE table_catalog = 'acme_prod'
    AND table_schema = 'finance_gold'
    AND table_name = 'silver_to_gold';

-- Query 2: Volume Anomaly Detection
-- Compare today's row count against the 7-day rolling average
-- Flag anomalies where today's count deviates more than 2 standard deviations
WITH daily_counts AS (
    SELECT
        DATE(processing_timestamp) AS record_date,
        COUNT(*) AS daily_row_count
    FROM acme_prod.finance_gold.silver_to_gold
    WHERE DATE(processing_timestamp) >= DATE_ADD(CURRENT_DATE(), -7)
    GROUP BY DATE(processing_timestamp)
),
stats AS (
    SELECT
        AVG(daily_row_count) AS avg_7d_count,
        STDDEV(daily_row_count) AS stddev_7d_count
    FROM daily_counts
    WHERE record_date < CURRENT_DATE()
),
today_count AS (
    SELECT COUNT(*) AS todays_row_count
    FROM acme_prod.finance_gold.silver_to_gold
    WHERE DATE(processing_timestamp) = CURRENT_DATE()
)
SELECT
    'acme_prod.finance_gold.silver_to_gold' AS table_name,
    t.todays_row_count,
    s.avg_7d_count,
    s.stddev_7d_count,
    ROUND((t.todays_row_count - s.avg_7d_count) / NULLIF(s.stddev_7d_count, 0), 2) AS z_score,
    CASE
        WHEN t.todays_row_count = 0
            THEN 'ALERT: No rows ingested today'
        WHEN ABS((t.todays_row_count - s.avg_7d_count) / NULLIF(s.stddev_7d_count, 0)) > 2
            THEN 'ALERT: Volume anomaly detected (>2 std deviations)'
        WHEN t.todays_row_count < s.avg_7d_count * 0.5
            THEN 'WARNING: Row count is less than 50% of 7-day average'
        ELSE 'OK: Volume within normal range'
    END AS volume_status
FROM today_count t
CROSS JOIN stats s;

-- Query 3: Schema Drift Detection
-- Compare current schema against expected column definitions
-- Detects added, removed, or type-changed columns
WITH expected_schema AS (
    SELECT column_name, data_type, ordinal_position
    FROM system.information_schema.columns
    WHERE table_catalog = 'acme_prod'
        AND table_schema = 'finance_gold'
        AND table_name = 'silver_to_gold'
),
schema_snapshot AS (
    -- This represents the last known good schema; in production, store this in a control table
    -- For detection, we compare against the information_schema history or a baseline table
    SELECT
        column_name,
        data_type,
        ordinal_position,
        last_altered
    FROM system.information_schema.columns
    WHERE table_catalog = 'acme_prod'
        AND table_schema = 'finance_gold'
        AND table_name = 'silver_to_gold'
)
SELECT
    'acme_prod.finance_gold.silver_to_gold' AS table_name,
    COUNT(*) AS total_columns,
    MAX(last_altered) AS last_schema_change,
    CASE
        WHEN TIMESTAMPDIFF(HOUR, MAX(last_altered), CURRENT_TIMESTAMP()) < 24
        THEN 'WARNING: Schema changed in the last 24 hours - review drift'
        ELSE 'OK: No recent schema changes detected'
    END AS schema_drift_status,
    COLLECT_LIST(
        CONCAT(column_name, ' (', data_type, ')')
    ) AS current_columns
FROM schema_snapshot;

-- Query 4: Null Rate Monitoring for Key Columns
-- Monitor null percentages across critical columns
-- Alert if null rate exceeds acceptable thresholds for financial data
SELECT
    'acme_prod.finance_gold.silver_to_gold' AS table_name,
    COUNT(*) AS total_rows,
    -- Key financial columns null rate analysis
    ROUND(100.0 * SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS transaction_id_null_pct,
    ROUND(100.0 * SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS account_id_null_pct,
    ROUND(100.0 * SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS amount_null_pct,
    ROUND(100.0 * SUM(CASE WHEN currency_code IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS currency_code_null_pct,
    ROUND(100.0 * SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS transaction_date_null_pct,
    ROUND(100.0 * SUM(CASE WHEN processing_timestamp IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS processing_timestamp_null_pct,
    -- Overall quality assessment
    CASE
        WHEN SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) > 0
            THEN 'CRITICAL: Primary key (transaction_id) contains NULLs'
        WHEN SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) > 0
            THEN 'CRITICAL: account_id contains NULLs - regulatory risk'
        WHEN (100.0 * SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) / COUNT(*)) > 1.0
            THEN 'ALERT: amount null rate exceeds 1% threshold'
        WHEN (100.0 * SUM(CASE WHEN currency_code IS NULL THEN 1 ELSE 0 END) / COUNT(*)) > 0.5
            THEN 'ALERT: currency_code null rate exceeds 0.5% threshold'
        WHEN (100.0 * SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) / COUNT(*)) > 0.1
            THEN 'WARNING: transaction_date null rate exceeds 0.1% threshold'
        ELSE 'OK: All key columns within acceptable null rate thresholds'
    END AS data_quality_status
FROM acme_prod.finance_gold.silver_to_gold
WHERE DATE(processing_timestamp) = CURRENT_DATE();