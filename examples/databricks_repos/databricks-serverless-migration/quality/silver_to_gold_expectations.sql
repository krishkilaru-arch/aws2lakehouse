-- SDP Expectations for silver_to_gold
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.silver_to_gold (
  CONSTRAINT valid_silver_to_gold_id EXPECT (silver_to_gold_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);