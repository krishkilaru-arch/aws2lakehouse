ALTER TABLE acme_prod.finance_gold.silver_to_gold SET TAGS ('domain' = 'finance', 'layer' = 'gold', 'classification' = 'internal', 'pipeline' = 'silver_to_gold');

COMMENT ON TABLE acme_prod.finance_gold.silver_to_gold IS 'Gold layer table in the finance domain, produced by the silver_to_gold pipeline. Contains refined, business-ready financial data transformed from the silver layer for downstream analytics and reporting.';

ALTER TABLE acme_prod.finance_gold.silver_to_gold ALTER COLUMN _ingested_at SET TAGS ('audit_column' = 'true', 'description' = 'Timestamp when the record was ingested into the platform');

ALTER TABLE acme_prod.finance_gold.silver_to_gold ALTER COLUMN _processed_at SET TAGS ('audit_column' = 'true', 'description' = 'Timestamp when the record was processed into the gold layer');

CREATE OR REPLACE FUNCTION acme_prod.finance_gold.silver_to_gold_environment_filter(env STRING)
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('finance_prod_access'), true, env = current_catalog());

ALTER TABLE acme_prod.finance_gold.silver_to_gold SET ROW FILTER acme_prod.finance_gold.silver_to_gold_environment_filter ON (environment);