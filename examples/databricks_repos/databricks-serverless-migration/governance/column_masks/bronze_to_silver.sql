-- Governance: bronze_to_silver
ALTER TABLE acme_prod.finance_bronze.bronze_to_silver SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.bronze_to_silver SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');