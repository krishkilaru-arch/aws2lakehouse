# Ingestion Patterns Used

> 1 distinct ingestion patterns across 5 pipelines

## Pattern Summary

| Source Type | Count | Pipelines |
|-------------|-------|-----------|
| auto_loader | 5 | bronze_to_silver, silver_to_gold, silver_transformation, bronze_ingestion, gold_aggregation |


## Pattern Details

### Auto Loader (5 pipelines)

**Pipelines:** bronze_to_silver, silver_to_gold, silver_transformation, bronze_ingestion, gold_aggregation

**Pattern:** See `src/pipelines/*/bronze/bronze_to_silver.py` for reference implementation.



## Common Configuration

All pipelines use:
- **Secrets:** `dbutils.secrets.get(scope, key)` — no hardcoded credentials
- **Parameterization:** `os.getenv("CATALOG")`, `os.getenv("SCHEMA")`, `os.getenv("TABLE")`
- **Audit columns:** `_ingested_at` (timestamp), `_source_file` (for file-based)
- **Schema evolution:** `mergeSchema=true` on all writes
- **Checkpointing:** `/Volumes/{catalog}/{schema}/_checkpoints/{pipeline}`
