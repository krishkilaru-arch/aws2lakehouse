-- Governance: daily_risk_aggregation
ALTER TABLE production.risk_bronze.daily_risk_aggregation SET TAGS ('data_classification' = 'mnpi');
ALTER TABLE production.risk_bronze.daily_risk_aggregation SET TAGS ('domain' = 'risk', 'owner' = 'risk-engineering@acmecapital.com');
ALTER TABLE production.risk_bronze.daily_risk_aggregation ALTER COLUMN var_amount SET TAGS ('mnpi' = 'true');
ALTER TABLE production.risk_bronze.daily_risk_aggregation ALTER COLUMN exposure SET TAGS ('mnpi' = 'true');
CREATE OR REPLACE FUNCTION production.risk_bronze.mask_daily_risk_aggregation_var_amount(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('risk_team') THEN val ELSE 0.00 END;
ALTER TABLE production.risk_bronze.daily_risk_aggregation ALTER COLUMN var_amount SET MASK production.risk_bronze.mask_daily_risk_aggregation_var_amount;
CREATE OR REPLACE FUNCTION production.risk_bronze.mask_daily_risk_aggregation_exposure(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('risk_team') THEN val ELSE 0.00 END;
ALTER TABLE production.risk_bronze.daily_risk_aggregation ALTER COLUMN exposure SET MASK production.risk_bronze.mask_daily_risk_aggregation_exposure;