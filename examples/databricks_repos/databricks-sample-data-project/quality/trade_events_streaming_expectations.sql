-- SDP Expectations for trade_events_streaming
CREATE OR REFRESH STREAMING TABLE acme_prod.risk_bronze.trade_events_streaming (
  CONSTRAINT valid_trade_events_streaming_id EXPECT (trade_events_streaming_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);