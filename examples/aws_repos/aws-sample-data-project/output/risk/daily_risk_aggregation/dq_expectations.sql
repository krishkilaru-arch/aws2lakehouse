-- SDP Expectations for daily_risk_aggregation
CREATE OR REFRESH STREAMING TABLE production.risk_bronze.daily_risk_aggregation (
  CONSTRAINT valid_daily_risk_aggregation_id EXPECT (daily_risk_aggregation_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);