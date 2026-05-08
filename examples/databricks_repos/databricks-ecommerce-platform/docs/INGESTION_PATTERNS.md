# Ingestion Patterns Used

> 1 distinct ingestion patterns across 1 pipelines

## Pattern Summary

| Source Type | Count | Pipelines |
|-------------|-------|-----------|
| delta_table | 1 | ecommerce_daily_etl |


## Pattern Details

### Delta Table (1 pipelines)

**Pipelines:** ecommerce_daily_etl

**Pattern:** See `src/pipelines/*/bronze/ecommerce_daily_etl.py` for reference implementation.



## Common Configuration

All pipelines use:
- **Secrets:** `dbutils.secrets.get(scope, key)` — no hardcoded credentials
- **Parameterization:** `os.getenv("CATALOG")`, `os.getenv("SCHEMA")`, `os.getenv("TABLE")`
- **Audit columns:** `_ingested_at` (timestamp), `_source_file` (for file-based)
- **Schema evolution:** `mergeSchema=true` on all writes
- **Checkpointing:** `/Volumes/{catalog}/{schema}/_checkpoints/{pipeline}`
