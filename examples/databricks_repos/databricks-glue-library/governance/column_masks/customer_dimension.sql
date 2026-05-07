-- Governance: customer_dimension
ALTER TABLE acme_prod.finance_bronze.customer_dimension SET TAGS ('data_classification' = 'confidential');
ALTER TABLE acme_prod.finance_bronze.customer_dimension SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');