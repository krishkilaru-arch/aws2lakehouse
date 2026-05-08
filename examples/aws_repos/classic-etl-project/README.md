# EcomCo Data Platform (Classic ETL)

## Architecture

```
PostgreSQL ──┐
MySQL ───────┤                    ┌─────────┐     ┌──────────┐
REST APIs ───┼── Airflow DAG ──▶ │   EMR   │ ──▶ │ Redshift │
S3 Files ────┤    (extract)       │ (Spark) │     │  (Star)  │
Salesforce ──┘                    └─────────┘     └──────────┘
                                   Bronze → Silver → Gold
```

## Pipeline: ecommerce_daily_etl

| Phase | Tool | Duration | Description |
|-------|------|----------|-------------|
| Extract | Airflow operators | ~20 min | 5 source systems → S3 staging |
| Bronze | EMR Spark | ~15 min | Clean, standardize, dedup |
| Silver | EMR Spark | ~25 min | Join, enrich, sessionize |
| Gold | EMR Spark | ~10 min | Star schema (facts + dims) |
| Load | S3 → Redshift COPY | ~8 min | Upsert to analytics schema |
| Validate | Python script | ~2 min | Row count + freshness checks |

**Total: ~80 minutes | SLA: 3 hours | Runs daily at 3:00 AM UTC**

## Costs: $37,300/month

| Service | Monthly Cost |
|---------|-------------|
| EMR (6-node cluster, daily spin-up) | $18,500 |
| Redshift (4-node ra3.4xlarge) | $12,000 |
| MWAA Airflow | $3,200 |
| S3 Storage (15TB) | $2,100 |
| Data Transfer | $1,500 |
