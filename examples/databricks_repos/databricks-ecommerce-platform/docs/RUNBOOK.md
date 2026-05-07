# Operations Runbook

> How to operate and troubleshoot the 1 pipelines in this project

## Quick Reference

| Action | Command |
|--------|---------|
| Deploy | `databricks bundle deploy --target prod` |
| Check status | `databricks jobs list --output JSON | grep acme-retail` |
| View runs | Workspace → Workflows → filter by tag:domain |
| Rerun failed | `databricks jobs run-now --job-id <id>` |
| View logs | Workspace → Job Run → click task → view notebook output |

## Pipeline-Specific Runbooks

## Analytics Domain

### ecommerce_daily_etl
- **Schedule:** `0 3 * * *`
- **SLA:** 60 minutes
- **Owner:** data-engineering@company.com
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
FROM ecommerce_prod._monitoring.pipeline_runs
GROUP BY 1 HAVING minutes_stale > 60;

-- Check DQ failures
SELECT * FROM ecommerce_prod._monitoring.dq_scores
WHERE score < 0.95 AND check_date = CURRENT_DATE;
```

## Escalation Path

1. **L1 (On-call engineer):** Rerun job, check obvious errors
2. **L2 (Domain engineer):** Investigate data issues, schema changes
3. **L3 (Platform team):** Cluster issues, permission problems, infra
4. **Compliance:** Any MNPI/PII-related alerts
