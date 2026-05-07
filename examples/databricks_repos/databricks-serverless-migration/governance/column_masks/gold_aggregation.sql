-- Governance: gold_aggregation
ALTER TABLE acme_prod.finance_bronze.gold_aggregation SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.gold_aggregation SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');