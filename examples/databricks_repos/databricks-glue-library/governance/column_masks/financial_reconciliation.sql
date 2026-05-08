-- Governance: financial_reconciliation
ALTER TABLE acme_prod.finance_bronze.financial_reconciliation SET TAGS ('data_classification' = 'confidential');
ALTER TABLE acme_prod.finance_bronze.financial_reconciliation SET TAGS ('domain' = 'finance', 'owner' = 'data-team@company.com');