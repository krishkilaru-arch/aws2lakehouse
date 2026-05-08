-- Governance: market_data_ingest_job
ALTER TABLE acme_prod.finance_bronze.market_data_ingest_job SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.market_data_ingest_job SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');