-- SDP Expectations for loan_application_etl
CREATE OR REFRESH STREAMING TABLE production.lending_bronze.loan_application_etl (
  CONSTRAINT valid_loan_application_etl_id EXPECT (loan_application_etl_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);