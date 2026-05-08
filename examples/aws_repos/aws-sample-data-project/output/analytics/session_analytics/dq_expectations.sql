-- SDP Expectations for session_analytics
CREATE OR REFRESH STREAMING TABLE production.analytics_bronze.session_analytics (
  CONSTRAINT valid_session_analytic_id EXPECT (session_analytic_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);