-- Governance: market_data_feed
ALTER TABLE acme_prod.risk_bronze.market_data_feed SET TAGS ('data_classification' = 'mnpi');
ALTER TABLE acme_prod.risk_bronze.market_data_feed SET TAGS ('domain' = 'risk', 'owner' = 'data-team@company.com');