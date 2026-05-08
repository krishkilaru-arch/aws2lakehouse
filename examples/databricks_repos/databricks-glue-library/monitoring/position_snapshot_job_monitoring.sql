-- Monitoring: position_snapshot_job
-- Freshness SLA check
SELECT 'position_snapshot_job' as pipeline, max(_ingested_at) as last_ingest,
  current_timestamp() - max(_ingested_at) as staleness,
  CASE WHEN current_timestamp() - max(_ingested_at) > INTERVAL 60 MINUTES THEN 'BREACH' ELSE 'OK' END as sla_status
FROM acme_prod.finance_bronze.position_snapshot_job;

-- Volume anomaly detection (7-day rolling average)
SELECT date(_ingested_at) as dt, count(*) as row_count,
  avg(count(*)) OVER (ORDER BY date(_ingested_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) as avg_7d,
  CASE
    WHEN count(*) < avg(count(*)) OVER (ORDER BY date(_ingested_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) * 0.5 THEN 'LOW_VOLUME'
    WHEN count(*) > avg(count(*)) OVER (ORDER BY date(_ingested_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) * 2.0 THEN 'HIGH_VOLUME'
    ELSE 'NORMAL'
  END as status
FROM acme_prod.finance_bronze.position_snapshot_job GROUP BY 1 ORDER BY 1 DESC LIMIT 14;
