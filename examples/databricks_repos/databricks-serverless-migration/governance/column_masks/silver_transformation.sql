-- Governance: silver_transformation
ALTER TABLE acme_prod.finance_bronze.silver_transformation SET TAGS ('data_classification' = 'confidential');
ALTER TABLE acme_prod.finance_bronze.silver_transformation SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');