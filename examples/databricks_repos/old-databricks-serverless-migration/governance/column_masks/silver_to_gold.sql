-- Governance: silver_to_gold
ALTER TABLE acme_prod.finance_bronze.silver_to_gold SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.silver_to_gold SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');