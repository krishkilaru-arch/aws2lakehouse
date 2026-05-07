-- SDP Expectations for trade_events_ingestion
CREATE OR REFRESH STREAMING TABLE production.risk_bronze.trade_events_ingestion (
  CONSTRAINT valid_trade_events_ingestion_id EXPECT (trade_events_ingestion_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);