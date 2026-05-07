-- Governance: order_flow_capture
ALTER TABLE production.risk_bronze.order_flow_capture SET TAGS ('data_classification' = 'internal');
ALTER TABLE production.risk_bronze.order_flow_capture SET TAGS ('domain' = 'risk', 'owner' = 'risk-engineering@acmecapital.com');