-- Governance: loan_application_etl
ALTER TABLE acme_prod.lending_bronze.loan_application_etl SET TAGS ('data_classification' = 'confidential');
ALTER TABLE acme_prod.lending_bronze.loan_application_etl SET TAGS ('domain' = 'lending', 'owner' = 'lending-team@company.com');