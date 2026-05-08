-- Governance: customer_360
ALTER TABLE acme_prod.customer_bronze.customer_360 SET TAGS ('data_classification' = 'confidential');
ALTER TABLE acme_prod.customer_bronze.customer_360 SET TAGS ('domain' = 'customer', 'owner' = 'customer-platform@company.com');