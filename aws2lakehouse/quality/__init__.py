"""
Data Quality & Observability Module.

Provides:
- Data quality rule engine (expectations, validation)
- Lineage tracking integration
- Alerting and monitoring framework
- SLA tracking
- Data freshness monitoring
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    BLOCKER = "blocker"


class RuleAction(Enum):
    WARN = "warn"        # Log warning, continue
    DROP = "drop"        # Drop failing rows
    QUARANTINE = "quarantine"  # Move to quarantine table
    FAIL = "fail"        # Fail the pipeline


@dataclass
class DQRule:
    """A data quality rule definition."""
    name: str
    description: str
    condition: str  # SQL condition that should be TRUE for valid rows
    action: RuleAction = RuleAction.WARN
    severity: Severity = Severity.WARNING
    threshold: float = 1.0  # Percentage of rows that must pass (0-1)
    tags: List[str] = field(default_factory=list)


@dataclass
class DQResult:
    """Result of a data quality check."""
    rule_name: str
    passed: bool
    total_rows: int
    passing_rows: int
    failing_rows: int
    pass_rate: float
    threshold: float
    action_taken: str
    timestamp: datetime = field(default_factory=datetime.now)


class DQFramework:
    """
    Data Quality Framework — Define, validate, and enforce quality rules.
    
    Usage:
        dq = DQFramework(catalog="production")
        
        # Define rules
        dq.add_rule(DQRule(
            name="valid_amount",
            description="Loan amount must be between 1000 and 150000",
            condition="loan_amount BETWEEN 1000 AND 150000",
            action=RuleAction.QUARANTINE,
            threshold=0.99
        ))
        
        dq.add_rule(DQRule(
            name="not_null_id",
            description="Application ID must not be null",
            condition="application_id IS NOT NULL",
            action=RuleAction.FAIL,
            threshold=1.0
        ))
        
        # Validate
        results = dq.validate_table("production.lending_bronze.loan_applications")
        
        # Generate SDP expectations
        sql = dq.generate_expectations_sql("loan_applications")
    """
    
    def __init__(self, catalog: str = "production"):
        self.catalog = catalog
        self.rules: Dict[str, List[DQRule]] = {}  # table -> rules
        self.results: List[DQResult] = []
    
    def add_rule(self, rule: DQRule, table: str = "default"):
        """Add a quality rule for a table."""
        if table not in self.rules:
            self.rules[table] = []
        self.rules[table].append(rule)
    
    def add_standard_rules(self, table: str, primary_key: str, required_columns: List[str] = None):
        """Add standard quality rules for a table."""
        required_columns = required_columns or []
        
        # Primary key not null
        self.add_rule(DQRule(
            name=f"{primary_key}_not_null",
            description=f"Primary key {primary_key} must not be null",
            condition=f"{primary_key} IS NOT NULL",
            action=RuleAction.FAIL,
            threshold=1.0
        ), table)
        
        # Primary key unique
        self.add_rule(DQRule(
            name=f"{primary_key}_unique",
            description=f"Primary key {primary_key} must be unique",
            condition=f"{primary_key} IS NOT NULL",  # Uniqueness checked separately
            action=RuleAction.DROP,
            threshold=1.0,
            tags=["uniqueness"]
        ), table)
        
        # Required columns not null
        for col in required_columns:
            self.add_rule(DQRule(
                name=f"{col}_not_null",
                description=f"Required column {col} must not be null",
                condition=f"{col} IS NOT NULL",
                action=RuleAction.QUARANTINE,
                threshold=0.99
            ), table)
    
    def validate_table(self, table_name: str, spark=None) -> List[DQResult]:
        """Run all rules against a table and return results."""
        # Find rules for this table
        short_name = table_name.split(".")[-1]
        rules = self.rules.get(short_name, self.rules.get("default", []))
        
        if not rules:
            logger.warning(f"No DQ rules defined for {table_name}")
            return []
        
        results = []
        
        if spark:
            total_rows = spark.table(table_name).count()
            
            for rule in rules:
                passing = spark.sql(
                    f"SELECT COUNT(*) as cnt FROM {table_name} WHERE {rule.condition}"
                ).first()[0]
                
                failing = total_rows - passing
                pass_rate = passing / total_rows if total_rows > 0 else 1.0
                
                result = DQResult(
                    rule_name=rule.name,
                    passed=pass_rate >= rule.threshold,
                    total_rows=total_rows,
                    passing_rows=passing,
                    failing_rows=failing,
                    pass_rate=pass_rate,
                    threshold=rule.threshold,
                    action_taken=rule.action.value if pass_rate < rule.threshold else "none"
                )
                results.append(result)
                self.results.append(result)
                
                if not result.passed:
                    logger.warning(
                        f"DQ FAIL: {rule.name} on {table_name} — "
                        f"{result.pass_rate:.1%} < {rule.threshold:.1%} threshold"
                    )
        
        return results
    
    def generate_expectations_sql(self, table: str) -> str:
        """Generate Spark Declarative Pipeline (SDP) expectations SQL."""
        rules = self.rules.get(table, [])
        
        if not rules:
            return f"-- No DQ rules defined for {table}"
        
        expectations = []
        for rule in rules:
            action_map = {
                RuleAction.WARN: "WARN",
                RuleAction.DROP: "DROP ROW",
                RuleAction.FAIL: "FAIL UPDATE",
                RuleAction.QUARANTINE: "DROP ROW"
            }
            action = action_map.get(rule.action, "WARN")
            expectations.append(
                f"  CONSTRAINT {rule.name} EXPECT ({rule.condition}) ON VIOLATION {action}"
            )
        
        expectations_str = ",\n".join(expectations)
        
        return f"""-- Data Quality Expectations for {table}
-- Generated by aws2lakehouse DQFramework

CREATE OR REFRESH STREAMING TABLE {self.catalog}.silver.{table} (
{expectations_str}
) AS
SELECT * FROM STREAM({self.catalog}.bronze.{table});
"""
    
    def generate_monitoring_dashboard_sql(self) -> str:
        """Generate SQL for DQ monitoring dashboard."""
        return f"""-- Data Quality Monitoring Views

CREATE OR REPLACE VIEW {self.catalog}.audit.dq_summary AS
SELECT
    table_name,
    rule_name,
    check_timestamp,
    total_rows,
    passing_rows,
    failing_rows,
    ROUND(passing_rows * 100.0 / NULLIF(total_rows, 0), 2) as pass_rate_pct,
    threshold_pct,
    CASE WHEN pass_rate >= threshold THEN 'PASS' ELSE 'FAIL' END as status
FROM {self.catalog}.audit.dq_results
WHERE check_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY check_timestamp DESC;
"""


class LineageTracker:
    """
    Data lineage tracking and documentation.
    
    Integrates with Unity Catalog lineage features and provides
    additional custom lineage for cross-system tracking.
    """
    
    def __init__(self, catalog: str = "production"):
        self.catalog = catalog
    
    def generate_lineage_table_sql(self) -> str:
        """Generate lineage tracking table."""
        return f"""-- Custom Lineage Tracking Table
CREATE TABLE IF NOT EXISTS {self.catalog}.audit.pipeline_lineage (
    lineage_id STRING GENERATED ALWAYS AS IDENTITY,
    pipeline_name STRING NOT NULL,
    source_system STRING,
    source_table STRING,
    target_table STRING,
    transformation_type STRING,  -- 'ingestion', 'transform', 'aggregate'
    columns_used ARRAY<STRING>,
    columns_produced ARRAY<STRING>,
    row_count_in BIGINT,
    row_count_out BIGINT,
    execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    job_run_id STRING,
    duration_seconds DOUBLE
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
"""
    
    def log_lineage(self, spark, pipeline_name: str, source: str, target: str, 
                    rows_in: int, rows_out: int, job_run_id: str = ""):
        """Log a lineage record."""
        spark.sql(f"""
            INSERT INTO {self.catalog}.audit.pipeline_lineage 
            (pipeline_name, source_table, target_table, row_count_in, row_count_out, job_run_id)
            VALUES ('{pipeline_name}', '{source}', '{target}', {rows_in}, {rows_out}, '{job_run_id}')
        """)


class AlertManager:
    """
    Alerting framework for pipeline monitoring.
    
    Alert channels: Email, Slack, PagerDuty
    """
    
    def __init__(self):
        self.alert_rules: List[Dict] = []
    
    def add_alert(
        self,
        name: str,
        condition: str,
        severity: Severity,
        channels: List[str],
        message_template: str = ""
    ):
        """Add an alert rule."""
        self.alert_rules.append({
            "name": name,
            "condition": condition,
            "severity": severity.value,
            "channels": channels,
            "message_template": message_template or f"Alert: {name} triggered"
        })
    
    def generate_alert_config(self) -> str:
        """Generate alert configuration YAML."""
        import json
        return json.dumps({"alerts": self.alert_rules}, indent=2)
    
    def add_standard_pipeline_alerts(self, pipeline_name: str, owner_email: str):
        """Add standard alerts for a pipeline."""
        self.add_alert(
            name=f"{pipeline_name}_failure",
            condition="job_status == 'FAILED'",
            severity=Severity.CRITICAL,
            channels=[owner_email, "data-engineering-alerts@company.com"],
            message_template=f"Pipeline {pipeline_name} FAILED — immediate attention required"
        )
        self.add_alert(
            name=f"{pipeline_name}_sla_breach",
            condition="runtime_minutes > sla_minutes",
            severity=Severity.WARNING,
            channels=[owner_email],
            message_template=f"Pipeline {pipeline_name} exceeded SLA"
        )
        self.add_alert(
            name=f"{pipeline_name}_dq_failure",
            condition="dq_pass_rate < threshold",
            severity=Severity.WARNING,
            channels=[owner_email, "data-quality@company.com"],
            message_template=f"Data quality check failed for {pipeline_name}"
        )



class FreshnessSLAMonitor:
    """
    Data Freshness SLA Monitoring.
    
    Detects stale data by checking last ingestion timestamp against SLA thresholds.
    Generates SQL views, alert queries, and monitoring dashboards.
    """
    
    def __init__(self, catalog: str = "production"):
        self.catalog = catalog
        self.monitored_tables: List[Dict] = []
    
    def add_table(self, table: str, sla_minutes: int, timestamp_col: str = "_ingested_at",
                  owner: str = "", alert_channels: List[str] = None):
        """Register a table for freshness monitoring."""
        self.monitored_tables.append({
            "table": table,
            "sla_minutes": sla_minutes,
            "timestamp_col": timestamp_col,
            "owner": owner,
            "alert_channels": alert_channels or [],
        })
    
    def generate_monitoring_view_sql(self, view_name: str = None) -> str:
        """Generate a unified freshness monitoring view."""
        if not self.monitored_tables:
            return "-- No tables registered for monitoring"
        
        view = view_name or f"{self.catalog}.observability.v_freshness_monitor"
        unions = []
        for t in self.monitored_tables:
            unions.append(f"""  SELECT
    '{t["table"]}' as table_name,
    '{t["owner"]}' as owner,
    {t["sla_minutes"]} as sla_minutes,
    max({t["timestamp_col"]}) as last_ingested,
    current_timestamp() as checked_at,
    TIMESTAMPDIFF(MINUTE, max({t["timestamp_col"]}), current_timestamp()) as staleness_minutes,
    CASE 
      WHEN TIMESTAMPDIFF(MINUTE, max({t["timestamp_col"]}), current_timestamp()) > {t["sla_minutes"]} THEN 'BREACHED'
      WHEN TIMESTAMPDIFF(MINUTE, max({t["timestamp_col"]}), current_timestamp()) > {t["sla_minutes"]} * 0.8 THEN 'WARNING'
      ELSE 'OK'
    END as sla_status
  FROM {t["table"]}""")
        
        sql = f"-- Freshness SLA Monitor\n"
        sql += f"CREATE OR REPLACE VIEW {view} AS\n"
        sql += "\nUNION ALL\n".join(unions)
        sql += ";"
        return sql
    
    def generate_alert_query(self) -> str:
        """Generate a query that returns only SLA breaches (for alert integration)."""
        view = f"{self.catalog}.observability.v_freshness_monitor"
        return f"""-- Alert query: returns breached SLAs only
SELECT table_name, owner, sla_minutes, staleness_minutes, sla_status,
  staleness_minutes - sla_minutes as breach_minutes
FROM {view}
WHERE sla_status = 'BREACHED'
ORDER BY breach_minutes DESC;
"""


class VolumeAnomalyDetector:
    """
    Data Volume Anomaly Detection.
    
    Detects unexpected changes in data volume (too few or too many rows)
    using rolling averages and configurable thresholds.
    """
    
    def __init__(self, catalog: str = "production"):
        self.catalog = catalog
        self.monitored_tables: List[Dict] = []
    
    def add_table(self, table: str, timestamp_col: str = "_ingested_at",
                  low_threshold: float = 0.5, high_threshold: float = 2.0,
                  lookback_days: int = 7, granularity: str = "day"):
        """Register a table for volume monitoring."""
        self.monitored_tables.append({
            "table": table,
            "timestamp_col": timestamp_col,
            "low_threshold": low_threshold,
            "high_threshold": high_threshold,
            "lookback_days": lookback_days,
            "granularity": granularity,
        })
    
    def generate_anomaly_view_sql(self, view_name: str = None) -> str:
        """Generate volume anomaly detection view."""
        if not self.monitored_tables:
            return "-- No tables registered"
        
        view = view_name or f"{self.catalog}.observability.v_volume_anomalies"
        unions = []
        
        for t in self.monitored_tables:
            low = t["low_threshold"]
            high = t["high_threshold"]
            lookback = t["lookback_days"]
            
            unions.append(f"""  SELECT
    '{t["table"]}' as table_name,
    date({t["timestamp_col"]}) as ingest_date,
    count(*) as row_count,
    avg(count(*)) OVER (
      ORDER BY date({t["timestamp_col"]}) 
      ROWS BETWEEN {lookback} PRECEDING AND 1 PRECEDING
    ) as rolling_avg,
    CASE
      WHEN count(*) < avg(count(*)) OVER (
        ORDER BY date({t["timestamp_col"]}) ROWS BETWEEN {lookback} PRECEDING AND 1 PRECEDING
      ) * {low} THEN 'LOW_VOLUME'
      WHEN count(*) > avg(count(*)) OVER (
        ORDER BY date({t["timestamp_col"]}) ROWS BETWEEN {lookback} PRECEDING AND 1 PRECEDING
      ) * {high} THEN 'HIGH_VOLUME'
      WHEN count(*) = 0 THEN 'ZERO_ROWS'
      ELSE 'NORMAL'
    END as volume_status
  FROM {t["table"]}
  GROUP BY date({t["timestamp_col"]})
  HAVING date({t["timestamp_col"]}) >= current_date() - INTERVAL {lookback + 7} DAYS""")
        
        sql = f"-- Volume Anomaly Detection\n"
        sql += f"CREATE OR REPLACE VIEW {view} AS\n"
        sql += "\nUNION ALL\n".join(unions)
        sql += "\nORDER BY ingest_date DESC;"
        return sql
    
    def generate_alert_query(self) -> str:
        """Generate query for volume anomaly alerts."""
        view = f"{self.catalog}.observability.v_volume_anomalies"
        return f"""-- Alert: Volume anomalies (today only)
SELECT table_name, ingest_date, row_count, rolling_avg, volume_status,
  ROUND(row_count / NULLIF(rolling_avg, 0), 2) as ratio_vs_avg
FROM {view}
WHERE ingest_date = current_date()
  AND volume_status != 'NORMAL'
ORDER BY table_name;
"""

    def generate_history_table_sql(self) -> str:
        """Generate a table to persist volume metrics for trend analysis."""
        return f"""-- Volume metrics history (append daily)
CREATE TABLE IF NOT EXISTS {self.catalog}.observability.volume_metrics_history (
  table_name STRING,
  metric_date DATE,
  row_count BIGINT,
  rolling_avg_7d DOUBLE,
  volume_status STRING,
  ratio_vs_avg DOUBLE,
  _recorded_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (metric_date)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
"""
