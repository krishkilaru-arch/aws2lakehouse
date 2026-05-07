-- Governance: compliance_report
ALTER TABLE production.compliance_bronze.compliance_report SET TAGS ('data_classification' = 'confidential');
ALTER TABLE production.compliance_bronze.compliance_report SET TAGS ('domain' = 'compliance', 'owner' = 'compliance-team@acmecapital.com');