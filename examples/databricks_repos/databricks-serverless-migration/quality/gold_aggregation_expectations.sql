-- SDP Expectations for gold_aggregation
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.gold_aggregation (
  CONSTRAINT valid_gold_aggregation_id EXPECT (gold_aggregation_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);