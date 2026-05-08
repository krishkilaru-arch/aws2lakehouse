-- Governance: bronze_ingestion
ALTER TABLE acme_prod.finance_bronze.bronze_ingestion SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.bronze_ingestion SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');