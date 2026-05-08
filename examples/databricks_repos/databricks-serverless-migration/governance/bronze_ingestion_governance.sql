ALTER TABLE acme_prod.finance_bronze.bronze_ingestion SET TAGS ('domain' = 'finance', 'layer' = 'bronze', 'classification' = 'internal', 'pipeline' = 'bronze_ingestion');

COMMENT ON TABLE acme_prod.finance_bronze.bronze_ingestion IS 'Bronze layer ingestion table for the finance domain. Contains raw data ingested from source systems with minimal transformation, serving as the landing zone for the bronze_ingestion pipeline.';

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ALTER COLUMN _ingested_at SET TAGS ('audit_column' = 'true', 'description' = 'Timestamp when the record was ingested into the bronze layer');

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ALTER COLUMN _processed_at SET TAGS ('audit_column' = 'true', 'description' = 'Timestamp when the record was processed by the ingestion pipeline');