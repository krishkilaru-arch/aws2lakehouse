-- SDP Expectations for financial_reconciliation
CREATE OR REFRESH STREAMING TABLE acme_prod.finance_bronze.financial_reconciliation (
  CONSTRAINT valid_financial_reconciliation_id EXPECT (financial_reconciliation_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);