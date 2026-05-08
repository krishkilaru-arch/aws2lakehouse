ALTER TABLE acme_prod.finance_gold.gold_aggregation SET TAGS ('domain' = 'finance', 'layer' = 'gold', 'classification' = 'internal', 'pipeline' = 'gold_aggregation');

COMMENT ON TABLE acme_prod.finance_gold.gold_aggregation IS 'Gold layer aggregation table in the finance domain. Contains aggregated financial metrics and KPIs derived from silver layer data, intended for internal reporting and analytics consumption.';

CREATE OR REPLACE FUNCTION acme_prod.finance_gold.gold_aggregation_environment_filter()
RETURNS BOOLEAN
RETURN (
  IS_ACCOUNT_GROUP_MEMBER('finance_prod_readers')
  OR IS_ACCOUNT_GROUP_MEMBER('finance_admin')
  OR current_user() IN (SELECT email FROM acme_prod.finance_gold.authorized_prod_users)
);

ALTER TABLE acme_prod.finance_gold.gold_aggregation SET ROW FILTER acme_prod.finance_gold.gold_aggregation_environment_filter ON ();

ALTER TABLE acme_prod.finance_gold.gold_aggregation ALTER COLUMN _ingested_at SET TAGS ('audit_column' = 'true', 'description' = 'Timestamp when record was ingested into the platform');

ALTER TABLE acme_prod.finance_gold.gold_aggregation ALTER COLUMN _processed_at SET TAGS ('audit_column' = 'true', 'description' = 'Timestamp when record was processed into the gold layer');