-- SDP Expectations for bronze_to_silver
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.bronze_to_silver (
  CONSTRAINT valid_bronze_to_silver_id EXPECT (bronze_to_silver_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);