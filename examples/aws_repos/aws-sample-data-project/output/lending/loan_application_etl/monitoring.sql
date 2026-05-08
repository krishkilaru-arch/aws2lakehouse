-- Monitoring: loan_application_etl
-- Freshness SLA check
SELECT 'loan_application_etl' as pipeline, max(_ingested_at) as last_ingest,
  current_timestamp() - max(_ingested_at) as staleness,
  CASE WHEN current_timestamp() - max(_ingested_at) > INTERVAL 30 MINUTES THEN 'BREACH' ELSE 'OK' END as sla_status
FROM production.lending_bronze.loan_application_etl;

-- Volume anomaly detection (7-day rolling average)
SELECT date(_ingested_at) as dt, count(*) as row_count,
  avg(count(*)) OVER (ORDER BY date(_ingested_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) as avg_7d,
  CASE
    WHEN count(*) < avg(count(*)) OVER (ORDER BY date(_ingested_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) * 0.5 THEN 'LOW_VOLUME'
    WHEN count(*) > avg(count(*)) OVER (ORDER BY date(_ingested_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) * 2.0 THEN 'HIGH_VOLUME'
    ELSE 'NORMAL'
  END as status
FROM production.lending_bronze.loan_application_etl GROUP BY 1 ORDER BY 1 DESC LIMIT 14;
