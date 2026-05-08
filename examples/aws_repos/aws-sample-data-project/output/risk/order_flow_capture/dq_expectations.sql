-- SDP Expectations for order_flow_capture
CREATE OR REFRESH STREAMING TABLE production.risk_bronze.order_flow_capture (
  CONSTRAINT valid_order_flow_capture_id EXPECT (order_flow_capture_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);