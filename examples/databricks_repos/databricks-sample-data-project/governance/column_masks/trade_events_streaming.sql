-- Governance: trade_events_streaming
ALTER TABLE acme_prod.risk_bronze.trade_events_streaming SET TAGS ('data_classification' = 'mnpi');
ALTER TABLE acme_prod.risk_bronze.trade_events_streaming SET TAGS ('domain' = 'risk', 'owner' = 'data-team@company.com');