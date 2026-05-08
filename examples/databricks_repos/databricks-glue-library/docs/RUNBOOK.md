# Operations Runbook

> How to operate and troubleshoot the 9 pipelines in this project

## Quick Reference

| Action | Command |
|--------|---------|
| Deploy | `databricks bundle deploy --target prod` |
| Check status | `databricks jobs list --output JSON | grep acme-capital` |
| View runs | Workspace → Workflows → filter by tag:domain |
| Rerun failed | `databricks jobs run-now --job-id <id>` |
| View logs | Workspace → Job Run → click task → view notebook output |

## Pipeline-Specific Runbooks

## Finance Domain

### feature_store_builder
- **Schedule:** `triggered`
- **SLA:** 60 minutes
- **Owner:** data-team@company.com
- **On failure:** Check job run in Workflows UI → Review error in notebook output
- **Rerun:** `databricks jobs run-now --job-id <job_id>`
- **Backfill:** Modify `CATALOG`/`SCHEMA` parameters, run manually

### customer_dimension
- **Schedule:** `triggered`
- **SLA:** 60 minutes
- **Owner:** data-team@company.com
- **On failure:** Check job run in Workflows UI → Review error in notebook output
- **Rerun:** `databricks jobs run-now --job-id <job_id>`
- **Backfill:** Modify `CATALOG`/`SCHEMA` parameters, run manually

### order_events_processing
- **Schedule:** `triggered`
- **SLA:** 60 minutes
- **Owner:** data-team@company.com
- **On failure:** Check job run in Workflows UI → Review error in notebook output
- **Rerun:** `databricks jobs run-now --job-id <job_id>`
- **Backfill:** Modify `CATALOG`/`SCHEMA` parameters, run manually

### financial_reconciliation
- **Schedule:** `triggered`
- **SLA:** 60 minutes
- **Owner:** data-team@company.com
- **On failure:** Check job run in Workflows UI → Review error in notebook output
- **Rerun:** `databricks jobs run-now --job-id <job_id>`
- **Backfill:** Modify `CATALOG`/`SCHEMA` parameters, run manually

### vendor_data_ingestion
- **Schedule:** `triggered`
- **SLA:** 60 minutes
- **Owner:** data-team@company.com
- **On failure:** Check job run in Workflows UI → Review error in notebook output
- **Rerun:** `databricks jobs run-now --job-id <job_id>`
- **Backfill:** Modify `CATALOG`/`SCHEMA` parameters, run manually

### market_data_ingest_job
- **Schedule:** `triggered`
- **SLA:** 60 minutes
- **Owner:** data-team@company.com
- **On failure:** Check job run in Workflows UI → Review error in notebook output
- **Rerun:** `databricks jobs run-now --job-id <job_id>`
- **Backfill:** Modify `CATALOG`/`SCHEMA` parameters, run manually

### position_snapshot_job
- **Schedule:** `triggered`
- **SLA:** 60 minutes
- **Owner:** data-team@company.com
- **On failure:** Check job run in Workflows UI → Review error in notebook output
- **Rerun:** `databricks jobs run-now --job-id <job_id>`
- **Backfill:** Modify `CATALOG`/`SCHEMA` parameters, run manually

### risk_aggregation_job
- **Schedule:** `triggered`
- **SLA:** 60 minutes
- **Owner:** data-team@company.com
- **On failure:** Check job run in Workflows UI → Review error in notebook output
- **Rerun:** `databricks jobs run-now --job-id <job_id>`
- **Backfill:** Modify `CATALOG`/`SCHEMA` parameters, run manually

### nav_calculation_job
- **Schedule:** `triggered`
- **SLA:** 60 minutes
- **Owner:** data-team@company.com
- **On failure:** Check job run in Workflows UI → Review error in notebook output
- **Rerun:** `databricks jobs run-now --job-id <job_id>`
- **Backfill:** Modify `CATALOG`/`SCHEMA` parameters, run manually


## Common Issues & Fixes

| Symptom | Cause | Fix |
|---------|-------|-----|
| "Table not found" | Schema not created | Run `governance/bootstrap.sql` |
| "Permission denied" | Missing grants | Check group membership in UC |
| Stale data (SLA breach) | Upstream delayed | Check dependency pipeline status |
| Volume anomaly (spike) | Source change | Verify with source team, check DQ |
| Streaming lag | Insufficient compute | Scale cluster or increase trigger interval |

## Monitoring Queries

```sql
-- Check pipeline freshness
SELECT pipeline_name, 
       TIMESTAMPDIFF(MINUTE, MAX(completed_at), CURRENT_TIMESTAMP()) as minutes_stale
FROM acme_prod._monitoring.pipeline_runs
GROUP BY 1 HAVING minutes_stale > 60;

-- Check DQ failures
SELECT * FROM acme_prod._monitoring.dq_scores
WHERE score < 0.95 AND check_date = CURRENT_DATE;
```

## Escalation Path

1. **L1 (On-call engineer):** Rerun job, check obvious errors
2. **L2 (Domain engineer):** Investigate data issues, schema changes
3. **L3 (Platform team):** Cluster issues, permission problems, infra
4. **Compliance:** Any MNPI/PII-related alerts
