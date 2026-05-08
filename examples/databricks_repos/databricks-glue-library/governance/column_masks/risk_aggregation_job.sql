-- Governance: risk_aggregation_job
ALTER TABLE acme_prod.finance_bronze.risk_aggregation_job SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.risk_aggregation_job SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');