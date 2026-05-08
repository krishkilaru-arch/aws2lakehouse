-- SDP Expectations for customer_churn_model
CREATE OR REFRESH STREAMING TABLE production.analytics_bronze.customer_churn_model (
  CONSTRAINT valid_customer_churn_model_id EXPECT (customer_churn_model_id IS NOT NULL) ON VIOLATION FAIL UPDATE
);