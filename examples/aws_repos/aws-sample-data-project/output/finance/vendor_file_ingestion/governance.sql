-- Governance: vendor_file_ingestion
ALTER TABLE production.finance_bronze.vendor_file_ingestion SET TAGS ('data_classification' = 'internal');
ALTER TABLE production.finance_bronze.vendor_file_ingestion SET TAGS ('domain' = 'finance', 'owner' = 'finance-ops@acmecapital.com');