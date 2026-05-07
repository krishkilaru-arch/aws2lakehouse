-- SDP Expectations for customer_dimension
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.customer_dimension (
  CONSTRAINT valid_customer_dimension_id EXPECT (customer_dimension_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);