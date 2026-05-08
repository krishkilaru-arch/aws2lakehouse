-- Unity Catalog Bootstrap for Acme Capital
CREATE CATALOG IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS production.market_data_bronze;
CREATE SCHEMA IF NOT EXISTS production.positions_bronze;
CREATE SCHEMA IF NOT EXISTS production.risk_silver;
CREATE SCHEMA IF NOT EXISTS production.nav_gold;
CREATE SCHEMA IF NOT EXISTS production.reference;
CREATE SCHEMA IF NOT EXISTS production.libraries;

-- MNPI Column Masks
CREATE OR REPLACE FUNCTION production.risk_silver.mask_pnl(val DOUBLE)
  RETURNS DOUBLE
  RETURN CASE WHEN is_account_group_member('risk_team') THEN val ELSE NULL END;

ALTER TABLE production.risk_silver.position_pnl ALTER COLUMN unrealized_pnl SET MASK production.risk_silver.mask_pnl;

-- Volume for library wheel
CREATE VOLUME IF NOT EXISTS production.libraries.wheels;
