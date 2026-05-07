-- SDP Expectations for market_data_feed
CREATE OR REFRESH STREAMING TABLE acme_prod.risk_bronze.market_data_feed (
  CONSTRAINT valid_market_data_feed_id EXPECT (market_data_feed_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);