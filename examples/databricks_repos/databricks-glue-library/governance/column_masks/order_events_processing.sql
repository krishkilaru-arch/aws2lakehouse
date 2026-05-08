-- Governance: order_events_processing
ALTER TABLE acme_prod.finance_bronze.order_events_processing SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.order_events_processing SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');