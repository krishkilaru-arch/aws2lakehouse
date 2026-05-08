ALTER TABLE acme_prod.finance_silver.silver_transformation SET TAGS ('domain' = 'finance', 'layer' = 'silver', 'classification' = 'internal', 'pipeline' = 'silver_transformation');

COMMENT ON TABLE acme_prod.finance_silver.silver_transformation IS 'Silver layer transformation table in the finance domain. Contains cleansed, validated, and conformed financial data transformed from bronze sources for downstream analytics and gold layer aggregation. Classification: internal.';

ALTER TABLE acme_prod.finance_silver.silver_transformation ALTER COLUMN _ingested_at SET TAGS ('audit_column' = 'true', 'description' = 'Timestamp when record was ingested from source');

ALTER TABLE acme_prod.finance_silver.silver_transformation ALTER COLUMN _processed_at SET TAGS ('audit_column' = 'true', 'description' = 'Timestamp when record was processed in silver transformation');

CREATE OR REPLACE FUNCTION acme_prod.finance_silver.silver_transformation_environment_filter(env STRING)
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('finance_prod_users'), true, env = current_catalog());

ALTER TABLE acme_prod.finance_silver.silver_transformation SET ROW FILTER acme_prod.finance_silver.silver_transformation_environment_filter ON (environment);