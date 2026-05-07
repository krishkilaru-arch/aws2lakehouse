-- Governance: daily_risk_aggregation
ALTER TABLE acme_prod.risk_bronze.daily_risk_aggregation SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.risk_bronze.daily_risk_aggregation SET TAGS ('domain' = 'risk', 'owner' = 'risk-engineering@company.com');