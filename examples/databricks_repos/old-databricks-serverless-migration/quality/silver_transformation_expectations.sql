-- SDP Expectations for silver_transformation
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.silver_transformation (
  CONSTRAINT valid_silver_transformation_id EXPECT (silver_transformation_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);