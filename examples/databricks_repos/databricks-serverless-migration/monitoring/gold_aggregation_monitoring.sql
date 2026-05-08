-- Query 1: Freshness Check
-- Alert if no new data has been written to the table in the last 2 hours
-- Uses the table's last modified timestamp from information_schema
SELECT
    'acme_prod.finance_gold.gold_aggregation' AS table_name,
    last_altered,
    TIMESTAMPDIFF(MINUTE, last_altered, CURRENT_TIMESTAMP()) AS minutes_since_update,
    CASE
        WHEN TIMESTAMPDIFF(MINUTE, last_altered, CURRENT_TIMESTAMP()) > 120
        THEN 'ALERT: Table has not been updated in over 2 hours'
        ELSE 'OK: Table is fresh'
    END AS freshness_status
FROM system.information_schema.tables
WHERE table_catalog = 'acme_prod'
    AND table_schema = 'finance_gold'
    AND table_name = 'gold_aggregation';

-- Query 2: Volume Anomaly Detection
-- Compare today's row count against the 7-day rolling average
-- Flag anomalies where deviation exceeds 25% from the average
WITH daily_counts AS (
    SELECT
        DATE(processing_timestamp) AS record_date,
        COUNT(*) AS daily_row_count
    FROM acme_prod.finance_gold.gold_aggregation
    WHERE DATE(processing_timestamp) >= DATE_ADD(CURRENT_DATE(), -7)
    GROUP BY DATE(processing_timestamp)
),
seven_day_avg AS (
    SELECT
        AVG(daily_row_count) AS avg_row_count,
        STDDEV(daily_row_count) AS stddev_row_count
    FROM daily_counts
    WHERE record_date < CURRENT_DATE()
),
today_count AS (
    SELECT COUNT(*) AS today_row_count
    FROM acme_prod.finance_gold.gold_aggregation
    WHERE DATE(processing_timestamp) = CURRENT_DATE()
)
SELECT
    t.today_row_count,
    a.avg_row_count,
    a.stddev_row_count,
    ROUND(((t.today_row_count - a.avg_row_count) / NULLIF(a.avg_row_count, 0)) * 100, 2) AS pct_deviation,
    CASE
        WHEN ABS((t.today_row_count - a.avg_row_count) / NULLIF(a.avg_row_count, 0)) > 0.25
        THEN 'ALERT: Volume anomaly detected - deviation exceeds 25%'
        WHEN t.today_row_count = 0
        THEN 'ALERT: No rows ingested today'
        ELSE 'OK: Volume within normal range'
    END AS volume_status
FROM today_count t
CROSS JOIN seven_day_avg a;

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
        AND table_schema = 'finance_gold'
        AND table_name = 'gold_aggregation'
),
baseline_schema AS (
    SELECT
        column_name,
        data_type,
        ordinal_position,
        is_nullable
    FROM acme_prod.finance_gold.schema_baseline
    WHERE table_name = 'gold_aggregation'
        AND baseline_date = (
            SELECT MAX(baseline_date)
            FROM acme_prod.finance_gold.schema_baseline
            WHERE table_name = 'gold_aggregation'
        )
)
SELECT
    COALESCE(c.column_name, b.column_name) AS column_name,
    b.data_type AS baseline_data_type,
    c.data_type AS current_data_type,
    CASE
        WHEN b.column_name IS NULL THEN 'ALERT: Column ADDED'
        WHEN c.column_name IS NULL THEN 'ALERT: Column REMOVED'
        WHEN b.data_type != c.data_type THEN 'ALERT: Data type CHANGED'
        WHEN b.is_nullable != c.is_nullable THEN 'ALERT: Nullability CHANGED'
        ELSE 'OK: No drift'
    END AS drift_status
FROM current_schema c
FULL OUTER JOIN baseline_schema b
    ON c.column_name = b.column_name
WHERE b.column_name IS NULL
    OR c.column_name IS NULL
    OR b.data_type != c.data_type
    OR b.is_nullable != c.is_nullable;

-- Query 4: Null Rate Monitoring for Key Columns
-- Monitor null rates across critical columns and alert if thresholds are exceeded
-- Financial data should have near-zero null rates for key business columns
SELECT
    'acme_prod.finance_gold.gold_aggregation' AS table_name,
    COUNT(*) AS total_rows,
    -- Key identifier columns (threshold: 0% nulls allowed)
    ROUND(SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS transaction_id_null_pct,
    ROUND(SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS account_id_null_pct,
    -- Key financial columns (threshold: 0.1% nulls allowed)
    ROUND(SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS amount_null_pct,
    ROUND(SUM(CASE WHEN currency_code IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS currency_code_null_pct,
    -- Temporal columns (threshold: 0% nulls allowed)
    ROUND(SUM(CASE WHEN processing_timestamp IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS processing_timestamp_null_pct,
    ROUND(SUM(CASE WHEN reporting_date IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS reporting_date_null_pct,
    -- Overall quality assessment
    CASE
        WHEN SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'CRITICAL: Null transaction_id detected'
        WHEN SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'CRITICAL: Null account_id detected'
        WHEN SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 0.1
        THEN 'ALERT: amount null rate exceeds 0.1% threshold'
        WHEN SUM(CASE WHEN currency_code IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 0.1
        THEN 'ALERT: currency_code null rate exceeds 0.1% threshold'
        WHEN SUM(CASE WHEN processing_timestamp IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'CRITICAL: Null processing_timestamp detected'
        ELSE 'OK: All null rates within acceptable thresholds'
    END AS null_quality_status
FROM acme_prod.finance_gold.gold_aggregation
WHERE DATE(processing_timestamp) = CURRENT_DATE();