-- SDP Expectations for customer_360
CREATE OR REFRESH STREAMING TABLE acme_prod.customer_bronze.customer_360 (
  CONSTRAINT valid_customer_360_id EXPECT (customer_360_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);