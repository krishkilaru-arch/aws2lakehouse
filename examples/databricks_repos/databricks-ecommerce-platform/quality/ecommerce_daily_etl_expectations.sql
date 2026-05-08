-- SDP Expectations for ecommerce_daily_etl
CREATE OR REFRESH STREAMING TABLE ecommerce_prod.analytics_bronze.ecommerce_daily_etl (
  CONSTRAINT valid_ecommerce_daily_etl_id EXPECT (ecommerce_daily_etl_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);