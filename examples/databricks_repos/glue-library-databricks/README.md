# Acme Capital Data Platform (Databricks)

Migrated from AWS Glue library pattern → Databricks with shared wheel on Volumes.

## Key Design Decision

The **acme_datalib** library is preserved as a wheel — same module structure,
same function signatures, same business logic. Only the infrastructure layer changed:
- `boto3` → `dbutils.secrets`
- `s3://` → `/Volumes/`
- `GlueContext` → native `SparkSession`

## Deploy

```bash
# Build library
cd lib && python setup.py bdist_wheel
databricks fs cp dist/*.whl /Volumes/production/libraries/

# Deploy pipelines
databricks bundle deploy --target dev
```
