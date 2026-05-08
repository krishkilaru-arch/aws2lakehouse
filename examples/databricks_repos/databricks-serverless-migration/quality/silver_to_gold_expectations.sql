ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT pk_not_null CHECK (id IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT transaction_id_not_null CHECK (transaction_id IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT account_id_not_null CHECK (account_id IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT transaction_date_not_null CHECK (transaction_date IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT amount_not_null CHECK (amount IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT currency_code_not_null CHECK (currency_code IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT status_not_null CHECK (status IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT created_at_not_null CHECK (created_at IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT updated_at_not_null CHECK (updated_at IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT amount_positive CHECK (amount >= 0);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT amount_reasonable_upper_bound CHECK (amount <= 1000000000);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT balance_valid_range CHECK (balance >= -1000000000 AND balance <= 1000000000);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT quantity_non_negative CHECK (quantity >= 0);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT exchange_rate_positive CHECK (exchange_rate > 0 AND exchange_rate < 10000);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT valid_status CHECK (status IN ('COMPLETED', 'PENDING', 'FAILED', 'CANCELLED', 'REVERSED'));

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT valid_currency_code CHECK (LENGTH(currency_code) = 3);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT transaction_date_not_future CHECK (transaction_date <= current_date());

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT transaction_date_reasonable CHECK (transaction_date >= '2000-01-01');

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT data_freshness_check CHECK (updated_at >= current_timestamp() - INTERVAL 24 HOURS);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT created_before_updated CHECK (created_at <= updated_at);

ALTER TABLE acme_prod.finance_gold.silver_to_gold
ADD CONSTRAINT processing_date_freshness CHECK (processing_date >= current_date() - INTERVAL 2 DAYS);