-- Governance: feature_store_builder
ALTER TABLE acme_prod.finance_bronze.feature_store_builder SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.feature_store_builder SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');