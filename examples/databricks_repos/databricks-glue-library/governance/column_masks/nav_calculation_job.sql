-- Governance: nav_calculation_job
ALTER TABLE acme_prod.finance_bronze.nav_calculation_job SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.nav_calculation_job SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');