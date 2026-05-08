-- SDP Expectations for feature_store_builder
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.feature_store_builder (
  CONSTRAINT valid_feature_store_builder_id EXPECT (feature_store_builder_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);