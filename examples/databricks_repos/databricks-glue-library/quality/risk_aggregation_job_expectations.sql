-- SDP Expectations for risk_aggregation_job
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.risk_aggregation_job (
  CONSTRAINT valid_risk_aggregation_job_id EXPECT (risk_aggregation_job_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);