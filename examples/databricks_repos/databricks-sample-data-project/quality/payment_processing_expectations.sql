-- SDP Expectations for payment_processing
CREATE OR REFRESH STREAMING TABLE acme_prod.lending_bronze.payment_processing (
  CONSTRAINT valid_payment_processing_id EXPECT (payment_processing_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);