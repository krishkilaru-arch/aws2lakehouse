-- SDP Expectations for bronze_ingestion
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.bronze_ingestion (
  CONSTRAINT valid_bronze_ingestion_id EXPECT (bronze_ingestion_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);