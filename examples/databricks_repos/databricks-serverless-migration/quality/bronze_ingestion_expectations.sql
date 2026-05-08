ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT pk_not_null CHECK (id IS NOT NULL);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT source_system_not_null CHECK (source_system IS NOT NULL);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT ingestion_timestamp_not_null CHECK (ingestion_timestamp IS NOT NULL);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT record_date_not_null CHECK (record_date IS NOT NULL);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT file_name_not_null CHECK (file_name IS NOT NULL);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT batch_id_not_null CHECK (batch_id IS NOT NULL);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT amount_valid_range CHECK (amount >= -999999999999.99 AND amount <= 999999999999.99);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT quantity_non_negative CHECK (quantity IS NULL OR quantity >= 0);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT record_count_valid CHECK (record_count IS NULL OR record_count >= 0);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT file_size_bytes_valid CHECK (file_size_bytes IS NULL OR file_size_bytes > 0);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT ingestion_freshness CHECK (ingestion_timestamp >= current_timestamp() - INTERVAL 24 HOURS);

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT record_date_not_future CHECK (record_date <= current_date());

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT record_date_reasonable CHECK (record_date >= '2000-01-01');

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT source_system_valid CHECK (source_system IN ('CORE_BANKING', 'TRADING_PLATFORM', 'RISK_ENGINE', 'PAYMENT_GATEWAY', 'GENERAL_LEDGER', 'MARKET_DATA', 'REGULATORY_FEED'));

ALTER TABLE acme_prod.finance_bronze.bronze_ingestion ADD CONSTRAINT ingestion_status_valid CHECK (ingestion_status IN ('RAW', 'RECEIVED', 'QUARANTINED', 'FAILED'));