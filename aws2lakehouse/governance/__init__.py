"""
Governance Module — Unity Catalog setup, access control, MNPI, and compliance.

Provides:
- Unity Catalog naming conventions and setup automation
- Role-based access control (RBAC) configuration
- MNPI (Material Non-Public Information) controls
- Data classification and tagging
- Audit logging configuration
- Multi-environment strategy (dev/staging/prod)
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class DataClassification(Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"       # PII, financial data
    MNPI = "mnpi"                   # Material Non-Public Information


class Environment(Enum):
    DEV = "dev"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class CatalogConfig:
    """Unity Catalog configuration for a catalog."""
    name: str
    environment: Environment
    owner: str
    comment: str = ""
    properties: dict[str, str] = field(default_factory=dict)
    schemas: list[str] = field(default_factory=list)


@dataclass
class AccessPolicy:
    """Access control policy definition."""
    principal: str          # user, group, or service principal
    privilege: str          # SELECT, MODIFY, CREATE, ALL PRIVILEGES, etc.
    securable_type: str     # CATALOG, SCHEMA, TABLE, VOLUME
    securable_name: str     # Fully qualified name
    condition: str = ""     # Row-level security condition


class UnityCatalogSetup:
    """
    Automates Unity Catalog configuration for migration.

    Naming Convention:
        Catalog:  {org}_{environment}         (e.g., apex_production)
        Schema:   {domain}_{layer}            (e.g., lending_bronze)
        Table:    {entity}                    (e.g., loan_applications)
        Volume:   {source_system}_{purpose}   (e.g., s3_raw_data)

    Environment Strategy:
        - dev:        apex_dev.{schema}.{table}
        - staging:    apex_staging.{schema}.{table}
        - production: apex_production.{schema}.{table}

    Usage:
        setup = UnityCatalogSetup(org="apex", environments=["dev", "staging", "production"])
        sql_statements = setup.generate_setup_sql()
        setup.apply(spark)
    """

    def __init__(
        self,
        org: str = "enterprise",
        environments: list[str] = None,
        domains: list[str] = None,
        layers: list[str] = None
    ):
        self.org = org
        self.environments = environments or ["dev", "staging", "production"]
        self.domains = domains or ["lending", "risk", "customer", "compliance", "analytics"]
        self.layers = layers or ["bronze", "silver", "gold"]

    def generate_setup_sql(self) -> str:
        """Generate SQL for complete Unity Catalog setup."""
        sql_parts = []

        sql_parts.append("-- ============================================================")
        sql_parts.append("-- UNITY CATALOG SETUP")
        sql_parts.append(f"-- Organization: {self.org}")
        sql_parts.append(f"-- Environments: {', '.join(self.environments)}")
        sql_parts.append("-- ============================================================\n")

        for env in self.environments:
            catalog_name = f"{self.org}_{env}"
            sql_parts.append(f"-- === {env.upper()} Environment ===")
            sql_parts.append(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
            sql_parts.append(f"  COMMENT '{self.org} {env} data catalog';\n")

            # Create schemas for each domain and layer
            for domain in self.domains:
                for layer in self.layers:
                    schema_name = f"{domain}_{layer}"
                    sql_parts.append(
                        f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}\n"
                        f"  COMMENT '{domain} domain - {layer} layer';"
                    )

            # Create utility schemas
            sql_parts.append(f"\nCREATE SCHEMA IF NOT EXISTS {catalog_name}.utilities")
            sql_parts.append("  COMMENT 'Shared utilities and reference data';")
            sql_parts.append(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.external")
            sql_parts.append("  COMMENT 'External volumes for cloud storage access';")
            sql_parts.append(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.audit")
            sql_parts.append("  COMMENT 'Audit logs and compliance tracking';\n")

        return "\n".join(sql_parts)

    def generate_volume_sql(self, s3_buckets: list[dict[str, str]]) -> str:
        """Generate Volume creation SQL for S3 bucket migration."""
        sql_parts = ["-- VOLUME CREATION FOR S3 MIGRATION\n"]

        for bucket in s3_buckets:
            bucket_name = bucket["name"]
            env = bucket.get("environment", "production")
            catalog = f"{self.org}_{env}"
            volume_name = bucket_name.replace("-", "_")

            sql_parts.append(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.external.{volume_name}")
            sql_parts.append(f"  LOCATION 's3://{bucket_name}'")
            sql_parts.append(f"  COMMENT 'Migrated from s3://{bucket_name}';\n")

        return "\n".join(sql_parts)

    def generate_access_policies(self) -> list[AccessPolicy]:
        """Generate standard access policies."""
        policies = []

        for env in self.environments:
            catalog = f"{self.org}_{env}"

            # Data Engineers: full access to bronze/silver, read gold
            policies.append(AccessPolicy(
                principal=f"{self.org}_data_engineers",
                privilege="ALL PRIVILEGES",
                securable_type="CATALOG",
                securable_name=catalog
            ))

            # Data Scientists: read silver/gold
            for domain in self.domains:
                for layer in ["silver", "gold"]:
                    policies.append(AccessPolicy(
                        principal=f"{self.org}_data_scientists",
                        privilege="SELECT",
                        securable_type="SCHEMA",
                        securable_name=f"{catalog}.{domain}_{layer}"
                    ))

            # Business Analysts: read gold only
            for domain in self.domains:
                policies.append(AccessPolicy(
                    principal=f"{self.org}_analysts",
                    privilege="SELECT",
                    securable_type="SCHEMA",
                    securable_name=f"{catalog}.{domain}_gold"
                ))

        return policies

    def generate_grants_sql(self) -> str:
        """Generate GRANT statements from policies."""
        policies = self.generate_access_policies()
        sql_parts = ["-- ACCESS CONTROL GRANTS\n"]

        for policy in policies:
            if policy.securable_name:
                sql_parts.append(
                    f"GRANT {policy.privilege} ON {policy.securable_type} "
                    f"{policy.securable_name} TO `{policy.principal}`;"
                )

        return "\n".join(sql_parts)


class MNPIController:
    """
    Material Non-Public Information (MNPI) access controls.

    For financial services compliance:
    - Tag tables/columns containing MNPI
    - Enforce access restrictions
    - Audit all MNPI access
    - Time-based access windows (embargo periods)
    """

    def __init__(self, catalog: str = "production"):
        self.catalog = catalog

    def generate_mnpi_tags_sql(self, mnpi_tables: list[dict[str, Any]]) -> str:
        """Generate SQL to tag MNPI data."""
        sql_parts = ["-- MNPI DATA CLASSIFICATION TAGS\n"]

        # Create tag taxonomy
        sql_parts.append("CREATE TAG IF NOT EXISTS data_classification;")
        sql_parts.append("CREATE TAG IF NOT EXISTS mnpi_category;")
        sql_parts.append("CREATE TAG IF NOT EXISTS embargo_hours;\n")

        for table_info in mnpi_tables:
            table = table_info["table"]
            category = table_info.get("category", "financial_results")
            embargo = table_info.get("embargo_hours", 24)
            columns = table_info.get("mnpi_columns", [])

            # Tag table
            sql_parts.append(f"ALTER TABLE {table} SET TAGS ('data_classification' = 'mnpi');")
            sql_parts.append(f"ALTER TABLE {table} SET TAGS ('mnpi_category' = '{category}');")
            sql_parts.append(f"ALTER TABLE {table} SET TAGS ('embargo_hours' = '{embargo}');")

            # Tag specific columns
            for col in columns:
                sql_parts.append(
                    f"ALTER TABLE {table} ALTER COLUMN {col} "
                    f"SET TAGS ('data_classification' = 'mnpi');"
                )

            sql_parts.append("")

        return "\n".join(sql_parts)

    def generate_row_filter_sql(self, table: str, embargo_column: str = "effective_date") -> str:
        """Generate row-level security for MNPI embargo periods."""
        return f"""-- Row-level filter for MNPI embargo on {table}
CREATE OR REPLACE FUNCTION {self.catalog}.utilities.mnpi_embargo_filter(embargo_date DATE)
RETURNS BOOLEAN
RETURN (
    -- Allow if user is in MNPI-authorized group
    IS_MEMBER('{self.catalog}_mnpi_authorized')
    -- Or if embargo period has passed (24 hours after effective_date)
    OR embargo_date < CURRENT_DATE()
);

ALTER TABLE {table} SET ROW FILTER
    {self.catalog}.utilities.mnpi_embargo_filter
    ON ({embargo_column});
"""

    def generate_audit_table_sql(self) -> str:
        """Generate audit logging table for MNPI access tracking."""
        return f"""-- MNPI Access Audit Table
CREATE TABLE IF NOT EXISTS {self.catalog}.audit.mnpi_access_log (
    access_id STRING GENERATED ALWAYS AS IDENTITY,
    user_email STRING NOT NULL,
    table_accessed STRING NOT NULL,
    columns_accessed ARRAY<STRING>,
    access_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    query_text STRING,
    justification STRING,
    approved_by STRING,
    access_type STRING  -- 'read', 'export', 'share'
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.logRetentionDuration' = '365 days'
)
COMMENT 'Audit trail for all MNPI data access - retention: 7 years';
"""




    def generate_column_mask_sql(self, table_configs: list[dict]) -> str:
        """
        Generate column-level masking functions and apply to tables.

        Column masking allows sensitive data to be automatically hidden from
        unauthorized users while remaining visible to approved groups.

        Args:
            table_configs: List of dicts with:
                - table: fully qualified table name
                - columns: list of dicts with {name, data_type, mask_expression, approved_group}

        Example:
            configs = [{
                "table": "prod.risk.trades",
                "columns": [
                    {"name": "trade_amount", "data_type": "DECIMAL(18,2)",
                     "mask_expression": "0.00", "approved_group": "mnpi_trading"},
                    {"name": "counterparty", "data_type": "STRING",
                     "mask_expression": "'REDACTED'", "approved_group": "mnpi_trading"},
                    {"name": "ssn", "data_type": "STRING",
                     "mask_expression": "CONCAT('***-**-', RIGHT(ssn, 4))", "approved_group": "pii_approved"},
                ]
            }]
        """
        stmts = ["-- Column Masking Functions (MNPI + PII Protection)"]
        stmts.append("-- Generated by aws2lakehouse MNPIController")
        stmts.append("")

        for config in table_configs:
            table = config["table"]
            parts = table.split(".")
            catalog = parts[0] if len(parts) == 3 else self.catalog
            schema = parts[1] if len(parts) == 3 else parts[0]
            table_name = parts[-1]

            stmts.append(f"-- Masks for {table}")

            for col in config.get("columns", []):
                col_name = col["name"]
                data_type = col.get("data_type", "STRING")
                mask_expr = col.get("mask_expression", "'***'")
                group = col.get("approved_group", "mnpi_approved")

                fn_name = f"{catalog}.{schema}.mask_{table_name}_{col_name}"

                stmts.append(f"""
CREATE OR REPLACE FUNCTION {fn_name}({col_name} {data_type})
RETURNS {data_type}
RETURN CASE
  WHEN is_account_group_member('{group}') THEN {col_name}
  ELSE {mask_expr}
END;

ALTER TABLE {table} ALTER COLUMN {col_name} SET MASK {fn_name};
""")
            # Tag columns
            for col in config.get("columns", []):
                stmts.append(f"ALTER TABLE {table} ALTER COLUMN {col['name']} SET TAGS ('masked' = 'true', 'sensitivity' = 'high');")
            stmts.append("")

        return "\n".join(stmts)

    def generate_dynamic_views_sql(self, view_configs: list[dict]) -> str:
        """
        Generate dynamic views that enforce data access policies.

        Dynamic views provide row-level and column-level security through SQL views
        that check user group membership at query time.

        Args:
            view_configs: List of dicts with:
                - source_table: fully qualified source table
                - view_name: fully qualified view name
                - row_filter: SQL condition for row-level access (optional)
                - column_rules: list of {name, default_expr, allowed_groups}
                - description: view description

        Example:
            configs = [{
                "source_table": "prod.risk.all_trades",
                "view_name": "prod.risk.v_trades_filtered",
                "row_filter": "trade_date >= current_date() - INTERVAL 90 DAYS OR is_account_group_member('full_history')",
                "column_rules": [
                    {"name": "pnl", "default_expr": "NULL", "allowed_groups": ["trading_desk", "risk_mgmt"]},
                    {"name": "client_name", "default_expr": "'[RESTRICTED]'", "allowed_groups": ["compliance", "legal"]},
                ]
            }]
        """
        stmts = ["-- Dynamic Views (Row + Column Level Security)"]
        stmts.append("-- These views enforce access policies at query time")
        stmts.append("")

        for config in view_configs:
            src = config["source_table"]
            view = config["view_name"]
            desc = config.get("description", f"Secured view of {src}")
            row_filter = config.get("row_filter")
            column_rules = config.get("column_rules", [])

            # Build column list
            col_exprs = []
            for rule in column_rules:
                groups = rule.get("allowed_groups", [])
                group_check = " OR ".join([f"is_account_group_member('{g}')" for g in groups])
                col_exprs.append(
                    f"  CASE WHEN {group_check} THEN {rule['name']} "
                    f"ELSE {rule['default_expr']} END AS {rule['name']}"
                )

            # Non-restricted columns
            select_clause = ", ".join(col_exprs) if col_exprs else "*"
            if col_exprs:
                select_clause = "  * EXCEPT(" + ", ".join([r["name"] for r in column_rules]) + "),\n" + ",\n".join(col_exprs)

            where_clause = f"\nWHERE {row_filter}" if row_filter else ""

            stmts.append(f"""CREATE OR REPLACE VIEW {view}
COMMENT '{desc}'
AS
SELECT
{select_clause}
FROM {src}{where_clause};
""")
            stmts.append("-- Grant access to view (restrict direct table access)")
            stmts.append(f"GRANT SELECT ON VIEW {view} TO `data_consumers`;")
            stmts.append(f"REVOKE SELECT ON TABLE {src} FROM `data_consumers`;")
            stmts.append("")

        return "\n".join(stmts)


class DataQualityGovernance:
    """Data quality rules and validation framework."""

    def generate_expectations_sql(self, table: str, rules: list[dict]) -> str:
        """Generate Delta Live Tables / SDP expectations."""
        expectations = []

        for rule in rules:
            name = rule["name"]
            condition = rule["condition"]
            action = rule.get("action", "warn")  # warn, drop, fail

            expectations.append(f"  CONSTRAINT {name} EXPECT ({condition}) ON VIOLATION {action.upper()}")

        expectations_str = ",\n".join(expectations)

        return f"""-- Data Quality Expectations for {table}
-- Apply in Spark Declarative Pipeline (SDP)

CREATE OR REFRESH STREAMING TABLE {table} (
{expectations_str}
) AS
SELECT * FROM STREAM(source_table);
"""
