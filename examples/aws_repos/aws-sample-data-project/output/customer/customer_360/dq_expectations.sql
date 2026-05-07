-- SDP Expectations for customer_360
CREATE OR REFRESH STREAMING TABLE production.customer_bronze.customer_360 (
  CONSTRAINT valid_customer_360_id EXPECT (customer_360_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);