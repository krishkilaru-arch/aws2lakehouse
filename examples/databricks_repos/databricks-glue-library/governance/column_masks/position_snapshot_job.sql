-- Governance: position_snapshot_job
ALTER TABLE acme_prod.finance_bronze.position_snapshot_job SET TAGS ('data_classification' = 'internal');
ALTER TABLE acme_prod.finance_bronze.position_snapshot_job SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');