-- SDP Expectations for vendor_file_ingestion
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.vendor_file_ingestion (
  CONSTRAINT valid_vendor_file_ingestion_id EXPECT (vendor_file_ingestion_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);