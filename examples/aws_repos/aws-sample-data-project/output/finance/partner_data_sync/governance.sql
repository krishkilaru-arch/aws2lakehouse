-- Governance: partner_data_sync
ALTER TABLE production.finance_bronze.partner_data_sync SET TAGS ('data_classification' = 'internal');
ALTER TABLE production.finance_bronze.partner_data_sync SET TAGS ('domain' = 'finance', 'owner' = 'finance-ops@acmecapital.com');