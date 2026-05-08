ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT pk_not_null CHECK (id IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT transaction_id_not_null CHECK (transaction_id IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT account_id_not_null CHECK (account_id IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT transaction_date_not_null CHECK (transaction_date IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT amount_not_null CHECK (amount IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT currency_code_not_null CHECK (currency_code IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT status_not_null CHECK (status IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT processed_timestamp_not_null CHECK (processed_timestamp IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT amount_valid_range CHECK (amount >= -999999999.99 AND amount <= 999999999.99);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT quantity_non_negative CHECK (quantity >= 0);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT exchange_rate_positive CHECK (exchange_rate > 0);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT balance_valid_range CHECK (balance >= -9999999999.99 AND balance <= 9999999999.99);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT interest_rate_valid_range CHECK (interest_rate >= 0 AND interest_rate <= 100);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT fee_amount_non_negative CHECK (fee_amount >= 0 AND fee_amount <= 999999.99);

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT transaction_date_not_future CHECK (transaction_date <= current_date());

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT transaction_date_freshness CHECK (transaction_date >= date_sub(current_date(), 90));

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT processed_timestamp_freshness CHECK (processed_timestamp >= date_sub(current_timestamp(), 7));

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT valid_currency_code CHECK (currency_code IN ('USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'HKD', 'SGD'));

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT valid_status CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED', 'REVERSED', 'CANCELLED', 'SETTLED'));

ALTER TABLE acme_prod.finance_silver.bronze_to_silver
ADD CONSTRAINT valid_account_id_format CHECK (length(account_id) >= 6 AND length(account_id) <= 34);