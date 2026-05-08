-- SDP Expectations for market_data_ingest_job
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.market_data_ingest_job (
  CONSTRAINT valid_market_data_ingest_job_id EXPECT (market_data_ingest_job_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);