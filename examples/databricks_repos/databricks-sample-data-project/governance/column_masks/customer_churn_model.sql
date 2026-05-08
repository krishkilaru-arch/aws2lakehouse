-- Governance: customer_churn_model
ALTER TABLE acme_prod.analytics_bronze.customer_churn_model SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.analytics_bronze.customer_churn_model SET TAGS ('domain' = 'analytics', 'owner' = 'data-team@company.com');