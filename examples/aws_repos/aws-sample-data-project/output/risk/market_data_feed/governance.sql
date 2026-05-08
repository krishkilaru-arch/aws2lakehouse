-- Governance: market_data_feed
ALTER TABLE production.risk_bronze.market_data_feed SET TAGS ('data_classification' = 'mnpi');
ALTER TABLE production.risk_bronze.market_data_feed SET TAGS ('domain' = 'risk', 'owner' = 'risk-engineering@acmecapital.com');
ALTER TABLE production.risk_bronze.market_data_feed ALTER COLUMN bid_price SET TAGS ('mnpi' = 'true');
ALTER TABLE production.risk_bronze.market_data_feed ALTER COLUMN ask_price SET TAGS ('mnpi' = 'true');
CREATE OR REPLACE FUNCTION production.risk_bronze.mask_market_data_feed_bid_price(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('risk_team') THEN val ELSE 0.00 END;
ALTER TABLE production.risk_bronze.market_data_feed ALTER COLUMN bid_price SET MASK production.risk_bronze.mask_market_data_feed_bid_price;
CREATE OR REPLACE FUNCTION production.risk_bronze.mask_market_data_feed_ask_price(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('risk_team') THEN val ELSE 0.00 END;
ALTER TABLE production.risk_bronze.market_data_feed ALTER COLUMN ask_price SET MASK production.risk_bronze.mask_market_data_feed_ask_price;