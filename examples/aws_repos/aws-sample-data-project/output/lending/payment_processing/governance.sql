-- Governance: payment_processing
ALTER TABLE production.lending_bronze.payment_processing SET TAGS ('data_classification' = 'confidential');
ALTER TABLE production.lending_bronze.payment_processing SET TAGS ('domain' = 'lending', 'owner' = 'lending-team@acmecapital.com');
ALTER TABLE production.lending_bronze.payment_processing ALTER COLUMN account_number SET TAGS ('mnpi' = 'true');
CREATE OR REPLACE FUNCTION production.lending_bronze.mask_payment_processing_account_number(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('lending_team') THEN val ELSE '[REDACTED]' END;
ALTER TABLE production.lending_bronze.payment_processing ALTER COLUMN account_number SET MASK production.lending_bronze.mask_payment_processing_account_number;