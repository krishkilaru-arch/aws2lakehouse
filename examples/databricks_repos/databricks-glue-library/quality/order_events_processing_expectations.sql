-- SDP Expectations for order_events_processing
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.order_events_processing (
  CONSTRAINT valid_order_events_processing_id EXPECT (order_events_processing_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);