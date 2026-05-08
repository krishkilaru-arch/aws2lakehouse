-- Governance: customer_360
ALTER TABLE production.customer_bronze.customer_360 SET TAGS ('data_classification' = 'confidential');
ALTER TABLE production.customer_bronze.customer_360 SET TAGS ('domain' = 'customer', 'owner' = 'customer-platform@acmecapital.com');
ALTER TABLE production.customer_bronze.customer_360 ALTER COLUMN email SET TAGS ('mnpi' = 'true');
ALTER TABLE production.customer_bronze.customer_360 ALTER COLUMN phone SET TAGS ('mnpi' = 'true');
ALTER TABLE production.customer_bronze.customer_360 ALTER COLUMN address SET TAGS ('mnpi' = 'true');
ALTER TABLE production.customer_bronze.customer_360 ALTER COLUMN dob SET TAGS ('mnpi' = 'true');
CREATE OR REPLACE FUNCTION production.customer_bronze.mask_customer_360_email(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('customer_team') THEN val ELSE '[REDACTED]' END;
ALTER TABLE production.customer_bronze.customer_360 ALTER COLUMN email SET MASK production.customer_bronze.mask_customer_360_email;
CREATE OR REPLACE FUNCTION production.customer_bronze.mask_customer_360_phone(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('customer_team') THEN val ELSE '[REDACTED]' END;
ALTER TABLE production.customer_bronze.customer_360 ALTER COLUMN phone SET MASK production.customer_bronze.mask_customer_360_phone;
CREATE OR REPLACE FUNCTION production.customer_bronze.mask_customer_360_address(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('customer_team') THEN val ELSE '[REDACTED]' END;
ALTER TABLE production.customer_bronze.customer_360 ALTER COLUMN address SET MASK production.customer_bronze.mask_customer_360_address;
CREATE OR REPLACE FUNCTION production.customer_bronze.mask_customer_360_dob(val STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('customer_team') THEN val ELSE '[REDACTED]' END;
ALTER TABLE production.customer_bronze.customer_360 ALTER COLUMN dob SET MASK production.customer_bronze.mask_customer_360_dob;