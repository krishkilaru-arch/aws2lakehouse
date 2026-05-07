-- SDP Expectations for position_snapshot_job
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.position_snapshot_job (
  CONSTRAINT valid_position_snapshot_job_id EXPECT (position_snapshot_job_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);