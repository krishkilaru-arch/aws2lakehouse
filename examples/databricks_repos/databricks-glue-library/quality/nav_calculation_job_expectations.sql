-- SDP Expectations for nav_calculation_job
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.nav_calculation_job (
  CONSTRAINT valid_nav_calculation_job_id EXPECT (nav_calculation_job_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);