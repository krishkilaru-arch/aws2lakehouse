-- Governance: loan_application_etl
ALTER TABLE production.lending_bronze.loan_application_etl SET TAGS ('data_classification' = 'confidential');
ALTER TABLE production.lending_bronze.loan_application_etl SET TAGS ('domain' = 'lending', 'owner' = 'lending-team@acmecapital.com');
ALTER TABLE production.lending_bronze.loan_application_etl ALTER COLUMN ssn SET TAGS ('mnpi' = 'true');
ALTER TABLE production.lending_bronze.loan_application_etl ALTER COLUMN income SET TAGS ('mnpi' = 'true');
ALTER TABLE production.lending_bronze.loan_application_etl ALTER COLUMN credit_score SET TAGS ('mnpi' = 'true');
CREATE OR REPLACE FUNCTION production.lending_bronze.mask_loan_application_etl_ssn(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('lending_team') THEN val ELSE '[REDACTED]' END;
ALTER TABLE production.lending_bronze.loan_application_etl ALTER COLUMN ssn SET MASK production.lending_bronze.mask_loan_application_etl_ssn;
CREATE OR REPLACE FUNCTION production.lending_bronze.mask_loan_application_etl_income(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('lending_team') THEN val ELSE '[REDACTED]' END;
ALTER TABLE production.lending_bronze.loan_application_etl ALTER COLUMN income SET MASK production.lending_bronze.mask_loan_application_etl_income;
CREATE OR REPLACE FUNCTION production.lending_bronze.mask_loan_application_etl_credit_score(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('lending_team') THEN val ELSE '[REDACTED]' END;
ALTER TABLE production.lending_bronze.loan_application_etl ALTER COLUMN credit_score SET MASK production.lending_bronze.mask_loan_application_etl_credit_score;