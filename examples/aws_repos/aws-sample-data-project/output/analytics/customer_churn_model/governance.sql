-- Governance: customer_churn_model
ALTER TABLE production.analytics_bronze.customer_churn_model SET TAGS ('data_classification' = 'internal');
ALTER TABLE production.analytics_bronze.customer_churn_model SET TAGS ('domain' = 'analytics', 'owner' = 'ml-team@acmecapital.com');