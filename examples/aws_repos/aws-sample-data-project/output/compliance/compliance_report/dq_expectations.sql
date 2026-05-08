-- SDP Expectations for compliance_report
CREATE OR REFRESH STREAMING TABLE production.compliance_bronze.compliance_report (
  CONSTRAINT valid_compliance_report_id EXPECT (compliance_report_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);