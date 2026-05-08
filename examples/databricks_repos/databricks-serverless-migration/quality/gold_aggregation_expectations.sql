ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT nn_record_id CHECK (record_id IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT nn_aggregation_date CHECK (aggregation_date IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT nn_entity_id CHECK (entity_id IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT nn_metric_name CHECK (metric_name IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT nn_metric_value CHECK (metric_value IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT nn_currency_code CHECK (currency_code IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT nn_reporting_period CHECK (reporting_period IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT nn_last_updated CHECK (last_updated IS NOT NULL);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_metric_value_range CHECK (metric_value >= -999999999999.99 AND metric_value <= 999999999999.99);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_total_amount_non_negative CHECK (total_amount >= 0);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_transaction_count_positive CHECK (transaction_count >= 0 AND transaction_count <= 2147483647);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_record_count_positive CHECK (record_count > 0);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_percentage_range CHECK (percentage_value >= 0.0 AND percentage_value <= 100.0);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_variance_reasonable CHECK (variance_amount >= -999999999999.99 AND variance_amount <= 999999999999.99);

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_aggregation_date_valid CHECK (aggregation_date >= '2000-01-01' AND aggregation_date <= current_date());

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_freshness_last_updated CHECK (last_updated >= dateadd(DAY, -2, current_timestamp()));

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_reporting_period_freshness CHECK (reporting_period >= dateadd(MONTH, -13, current_date()));

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_currency_code_valid CHECK (currency_code IN ('USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'HKD', 'SGD', 'CNY'));

ALTER TABLE acme_prod.finance_gold.gold_aggregation ADD CONSTRAINT chk_aggregation_level_valid CHECK (aggregation_level IN ('DAILY', 'WEEKLY', 'MONTHLY', 'QUARTERLY', 'ANNUAL'));