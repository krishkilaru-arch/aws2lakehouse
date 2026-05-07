-- Governance: session_analytics
ALTER TABLE production.analytics_bronze.session_analytics SET TAGS ('data_classification' = 'internal');
ALTER TABLE production.analytics_bronze.session_analytics SET TAGS ('domain' = 'analytics', 'owner' = 'analytics-team@acmecapital.com');