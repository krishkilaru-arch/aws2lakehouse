-- Governance: trade_events_ingestion
ALTER TABLE production.risk_bronze.trade_events_ingestion SET TAGS ('data_classification' = 'mnpi');
ALTER TABLE production.risk_bronze.trade_events_ingestion SET TAGS ('domain' = 'risk', 'owner' = 'risk-engineering@acmecapital.com');
ALTER TABLE production.risk_bronze.trade_events_ingestion ALTER COLUMN notional_amount SET TAGS ('mnpi' = 'true');
ALTER TABLE production.risk_bronze.trade_events_ingestion ALTER COLUMN pnl SET TAGS ('mnpi' = 'true');
ALTER TABLE production.risk_bronze.trade_events_ingestion ALTER COLUMN counterparty_name SET TAGS ('mnpi' = 'true');
CREATE OR REPLACE FUNCTION production.risk_bronze.mask_trade_events_ingestion_notional_amount(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('risk_team') THEN val ELSE 0.00 END;
ALTER TABLE production.risk_bronze.trade_events_ingestion ALTER COLUMN notional_amount SET MASK production.risk_bronze.mask_trade_events_ingestion_notional_amount;
CREATE OR REPLACE FUNCTION production.risk_bronze.mask_trade_events_ingestion_pnl(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('risk_team') THEN val ELSE 0.00 END;
ALTER TABLE production.risk_bronze.trade_events_ingestion ALTER COLUMN pnl SET MASK production.risk_bronze.mask_trade_events_ingestion_pnl;
CREATE OR REPLACE FUNCTION production.risk_bronze.mask_trade_events_ingestion_counterparty_name(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('risk_team') THEN val ELSE 0.00 END;
ALTER TABLE production.risk_bronze.trade_events_ingestion ALTER COLUMN counterparty_name SET MASK production.risk_bronze.mask_trade_events_ingestion_counterparty_name;