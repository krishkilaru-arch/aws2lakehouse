ALTER TABLE acme_prod.finance_silver.bronze_to_silver SET TAGS ('domain' = 'finance', 'layer' = 'silver', 'classification' = 'internal', 'pipeline' = 'bronze_to_silver');

COMMENT ON TABLE acme_prod.finance_silver.bronze_to_silver IS 'Silver layer table containing cleansed and conformed data transformed from the bronze layer via the bronze_to_silver pipeline. This table serves as the curated intermediate layer for downstream finance analytics and reporting.';

ALTER TABLE acme_prod.finance_silver.bronze_to_silver ALTER COLUMN _ingested_at SET TAGS ('audit_column' = 'true', 'description' = 'Timestamp when the record was ingested from source into bronze layer');

ALTER TABLE acme_prod.finance_silver.bronze_to_silver ALTER COLUMN _processed_at SET TAGS ('audit_column' = 'true', 'description' = 'Timestamp when the record was processed and promoted to silver layer');

CREATE OR REPLACE FUNCTION acme_prod.finance_silver.bronze_to_silver_environment_filter(environment_col STRING)
RETURN environment_col = current_catalog();

ALTER TABLE acme_prod.finance_silver.bronze_to_silver SET ROW FILTER acme_prod.finance_silver.bronze_to_silver_environment_filter ON (environment);