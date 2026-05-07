# Ingestion Patterns Used

> 2 distinct ingestion patterns across 9 pipelines

## Pattern Summary

| Source Type | Count | Pipelines |
|-------------|-------|-----------|
| auto_loader | 5 | feature_store_builder, order_events_processing, vendor_data_ingestion, market_data_ingest_job, risk_aggregation_job |
| jdbc | 4 | customer_dimension, financial_reconciliation, position_snapshot_job, nav_calculation_job |


## Pattern Details

### Auto Loader (5 pipelines)

**Pipelines:** feature_store_builder, order_events_processing, vendor_data_ingestion, market_data_ingest_job, risk_aggregation_job

**Pattern:** See `src/pipelines/*/bronze/feature_store_builder.py` for reference implementation.


### Jdbc (4 pipelines)

**Pipelines:** customer_dimension, financial_reconciliation, position_snapshot_job, nav_calculation_job

**Pattern:** See `src/pipelines/*/bronze/customer_dimension.py` for reference implementation.



## Common Configuration

All pipelines use:
- **Secrets:** `dbutils.secrets.get(scope, key)` — no hardcoded credentials
- **Parameterization:** `os.getenv("CATALOG")`, `os.getenv("SCHEMA")`, `os.getenv("TABLE")`
- **Audit columns:** `_ingested_at` (timestamp), `_source_file` (for file-based)
- **Schema evolution:** `mergeSchema=true` on all writes
- **Checkpointing:** `/Volumes/{catalog}/{schema}/_checkpoints/{pipeline}`
