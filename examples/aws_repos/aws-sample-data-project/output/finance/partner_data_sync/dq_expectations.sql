-- SDP Expectations for partner_data_sync
CREATE OR REFRESH STREAMING TABLE production.finance_bronze.partner_data_sync (
  CONSTRAINT valid_partner_data_sync_id EXPECT (partner_data_sync_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);