-- Governance: vendor_data_ingestion
ALTER TABLE acme_prod.finance_bronze.vendor_data_ingestion SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.vendor_data_ingestion SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');