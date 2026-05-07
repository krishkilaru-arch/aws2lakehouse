"""
Wave Planning Dashboard — SQL views for migration tracking.

Creates views and tables for:
- Pipeline inventory dashboard
- Wave progress tracking
- Risk heatmap
- Effort estimation
- Dependency visualization
"""

from typing import Dict, List


class WavePlanningDashboard:
    """Generates SQL for a migration wave planning dashboard."""
    
    def __init__(self, catalog: str = "production", schema: str = "migration_tracking"):
        self.catalog = catalog
        self.schema = schema
        self.fqn = f"{catalog}.{schema}"
    
    def generate_all_sql(self) -> str:
        """Generate complete wave planning dashboard SQL."""
        return "\n\n".join([
            self._schema_setup(),
            self._pipeline_inventory_table(),
            self._wave_assignments_table(),
            self._migration_progress_table(),
            self._inventory_summary_view(),
            self._wave_progress_view(),
            self._risk_heatmap_view(),
            self._effort_burndown_view(),
            self._dependency_view(),
        ])
    
    def _schema_setup(self) -> str:
        return f"""-- Wave Planning Dashboard
CREATE SCHEMA IF NOT EXISTS {self.fqn};
USE SCHEMA {self.fqn};"""
    
    def _pipeline_inventory_table(self) -> str:
        return f"""
-- Pipeline Inventory (populated by discovery phase)
CREATE TABLE IF NOT EXISTS {self.fqn}.pipeline_inventory (
  pipeline_id STRING,
  pipeline_name STRING,
  domain STRING,
  pipeline_type STRING,          -- emr_batch, glue_etl, step_function, airflow_dag
  source_system STRING,          -- kafka, s3, mongodb, postgresql, snowflake
  target_layer STRING,           -- bronze, silver, gold
  complexity_score DECIMAL(5,1), -- 0-100
  complexity_category STRING,    -- simple, medium, complex, critical
  latency_class STRING,          -- batch, nrt, streaming
  business_impact STRING,        -- low, medium, high, critical
  mnpi_classification STRING,    -- public, internal, confidential, mnpi
  sla_minutes INT,
  estimated_hours DECIMAL(5,1),
  recommended_approach STRING,   -- lift_and_shift, refactor, rewrite
  owner STRING,
  dependencies ARRAY<STRING>,
  wave_number INT,
  migration_status STRING,       -- not_started, in_progress, validating, complete, blocked
  notes STRING,
  _created_at TIMESTAMP DEFAULT current_timestamp(),
  _updated_at TIMESTAMP
) USING DELTA;"""
    
    def _wave_assignments_table(self) -> str:
        return f"""
-- Wave Configuration
CREATE TABLE IF NOT EXISTS {self.fqn}.wave_config (
  wave_number INT,
  wave_name STRING,
  start_date DATE,
  target_end_date DATE,
  actual_end_date DATE,
  status STRING,                 -- planned, active, complete
  pipeline_count INT,
  total_effort_hours DECIMAL(8,1),
  risk_level STRING,             -- low, medium, high
  notes STRING
) USING DELTA;"""
    
    def _migration_progress_table(self) -> str:
        return f"""
-- Daily Progress Log (append daily)
CREATE TABLE IF NOT EXISTS {self.fqn}.migration_progress (
  log_date DATE,
  wave_number INT,
  pipelines_not_started INT,
  pipelines_in_progress INT,
  pipelines_validating INT,
  pipelines_complete INT,
  pipelines_blocked INT,
  effort_remaining_hours DECIMAL(8,1),
  blockers STRING,
  _recorded_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
PARTITIONED BY (log_date);"""
    
    def _inventory_summary_view(self) -> str:
        return f"""
-- Dashboard: Inventory Summary (tile metrics)
CREATE OR REPLACE VIEW {self.fqn}.v_inventory_summary AS
SELECT
  COUNT(*) as total_pipelines,
  COUNT(CASE WHEN complexity_category = 'simple' THEN 1 END) as simple_count,
  COUNT(CASE WHEN complexity_category = 'medium' THEN 1 END) as medium_count,
  COUNT(CASE WHEN complexity_category = 'complex' THEN 1 END) as complex_count,
  COUNT(CASE WHEN complexity_category = 'critical' THEN 1 END) as critical_count,
  ROUND(SUM(estimated_hours), 0) as total_effort_hours,
  COUNT(DISTINCT domain) as domains,
  COUNT(CASE WHEN mnpi_classification = 'mnpi' THEN 1 END) as mnpi_pipelines,
  ROUND(AVG(complexity_score), 1) as avg_complexity
FROM {self.fqn}.pipeline_inventory;"""
    
    def _wave_progress_view(self) -> str:
        return f"""
-- Dashboard: Wave Progress (bar chart)
CREATE OR REPLACE VIEW {self.fqn}.v_wave_progress AS
SELECT
  w.wave_number,
  w.wave_name,
  w.status as wave_status,
  COUNT(p.pipeline_id) as total,
  COUNT(CASE WHEN p.migration_status = 'complete' THEN 1 END) as completed,
  COUNT(CASE WHEN p.migration_status = 'in_progress' THEN 1 END) as in_progress,
  COUNT(CASE WHEN p.migration_status = 'blocked' THEN 1 END) as blocked,
  ROUND(
    COUNT(CASE WHEN p.migration_status = 'complete' THEN 1 END) * 100.0 
    / NULLIF(COUNT(*), 0), 1
  ) as completion_pct
FROM {self.fqn}.wave_config w
LEFT JOIN {self.fqn}.pipeline_inventory p ON p.wave_number = w.wave_number
GROUP BY w.wave_number, w.wave_name, w.status
ORDER BY w.wave_number;"""
    
    def _risk_heatmap_view(self) -> str:
        return f"""
-- Dashboard: Risk Heatmap (domain x complexity)
CREATE OR REPLACE VIEW {self.fqn}.v_risk_heatmap AS
SELECT
  domain,
  complexity_category,
  business_impact,
  COUNT(*) as pipeline_count,
  ROUND(AVG(complexity_score), 1) as avg_score,
  SUM(estimated_hours) as total_hours,
  CASE
    WHEN business_impact = 'critical' AND complexity_category IN ('complex', 'critical') THEN 'RED'
    WHEN business_impact IN ('high', 'critical') OR complexity_category = 'complex' THEN 'AMBER'
    ELSE 'GREEN'
  END as risk_color
FROM {self.fqn}.pipeline_inventory
GROUP BY domain, complexity_category, business_impact
ORDER BY 
  CASE risk_color WHEN 'RED' THEN 1 WHEN 'AMBER' THEN 2 ELSE 3 END,
  pipeline_count DESC;"""
    
    def _effort_burndown_view(self) -> str:
        return f"""
-- Dashboard: Effort Burndown (line chart)
CREATE OR REPLACE VIEW {self.fqn}.v_effort_burndown AS
SELECT
  log_date,
  SUM(effort_remaining_hours) as total_remaining_hours,
  SUM(pipelines_complete) as cumulative_complete,
  SUM(pipelines_blocked) as currently_blocked
FROM {self.fqn}.migration_progress
GROUP BY log_date
ORDER BY log_date;"""
    
    def _dependency_view(self) -> str:
        return f"""
-- Dashboard: Dependency Map
CREATE OR REPLACE VIEW {self.fqn}.v_dependencies AS
SELECT
  p.pipeline_name as pipeline,
  p.domain,
  p.wave_number,
  p.migration_status,
  dep.dependency as depends_on_pipeline,
  dep_p.migration_status as dependency_status,
  CASE 
    WHEN dep_p.migration_status = 'complete' THEN 'RESOLVED'
    WHEN dep_p.migration_status = 'blocked' THEN 'BLOCKED'
    ELSE 'PENDING'
  END as dependency_state
FROM {self.fqn}.pipeline_inventory p
LATERAL VIEW explode(p.dependencies) dep AS dependency
LEFT JOIN {self.fqn}.pipeline_inventory dep_p 
  ON dep_p.pipeline_name = dep.dependency;"""
