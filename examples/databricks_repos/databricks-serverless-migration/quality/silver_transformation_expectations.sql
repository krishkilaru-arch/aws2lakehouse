ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT nn_transaction_id CHECK (transaction_id IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT nn_account_id CHECK (account_id IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT nn_transaction_date CHECK (transaction_date IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT nn_amount CHECK (amount IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT nn_currency_code CHECK (currency_code IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT nn_transaction_type CHECK (transaction_type IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT nn_processed_timestamp CHECK (processed_timestamp IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT nn_source_system CHECK (source_system IS NOT NULL);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_amount_valid_range CHECK (amount >= -999999999.99 AND amount <= 999999999.99);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_amount_not_zero CHECK (amount != 0);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_exchange_rate_positive CHECK (exchange_rate > 0 AND exchange_rate < 10000);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_quantity_non_negative CHECK (quantity >= 0);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_balance_valid_range CHECK (running_balance >= -9999999999.99 AND running_balance <= 9999999999.99);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_fee_amount_non_negative CHECK (fee_amount >= 0 AND fee_amount <= 999999.99);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_tax_amount_non_negative CHECK (tax_amount >= 0 AND tax_amount <= 999999.99);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_transaction_date_not_future CHECK (transaction_date <= current_date());

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_transaction_date_reasonable CHECK (transaction_date >= '2000-01-01');

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_processed_timestamp_freshness CHECK (processed_timestamp >= current_timestamp() - INTERVAL 72 HOURS);

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_ingestion_not_future CHECK (ingestion_timestamp <= current_timestamp());

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_currency_code_valid CHECK (currency_code IN ('USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'HKD', 'SGD'));

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_transaction_type_valid CHECK (transaction_type IN ('CREDIT', 'DEBIT', 'TRANSFER', 'REVERSAL', 'ADJUSTMENT', 'FEE', 'INTEREST', 'DIVIDEND'));

ALTER TABLE acme_prod.finance_silver.silver_transformation ADD CONSTRAINT chk_status_valid CHECK (status IN ('COMPLETED', 'PENDING', 'FAILED', 'REVERSED', 'SETTLED'));