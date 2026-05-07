-- Governance: ecommerce_daily_etl
ALTER TABLE ecommerce_prod.analytics_bronze.ecommerce_daily_etl SET TAGS ('data_classification' = 'internal');
ALTER TABLE ecommerce_prod.analytics_bronze.ecommerce_daily_etl SET TAGS ('domain' = 'analytics', 'owner' = 'data-engineering@company.com');