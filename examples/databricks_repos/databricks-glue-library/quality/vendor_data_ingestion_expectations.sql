-- SDP Expectations for vendor_data_ingestion
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.vendor_data_ingestion (
  CONSTRAINT valid_vendor_data_ingestion_id EXPECT (vendor_data_ingestion_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);