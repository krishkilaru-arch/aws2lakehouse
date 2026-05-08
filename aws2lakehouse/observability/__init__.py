"""
Observability & Monitoring Framework.

Pre-built SQL dashboard views, idempotent pipeline patterns,
checkpointing utilities, and operational monitoring.
"""


class MonitoringDashboard:
    """
    Pre-built SQL views for a Pipeline Health monitoring dashboard.

    Creates a unified observability layer with:
    - Pipeline execution history
    - Freshness SLA tracking
    - Volume anomaly detection
    - Data quality scores
    - Cost tracking
    - Lineage summary
    """

    def __init__(self, catalog: str = "production", schema: str = "observability"):
        self.catalog = catalog
        self.schema = schema
        self.fqn = f"{catalog}.{schema}"

    def generate_all_sql(self) -> str:
        """Generate complete monitoring dashboard SQL."""
        sections = [
            self._schema_setup(),
            self._execution_history_table(),
            self._freshness_view(),
            self._volume_view(),
            self._dq_scores_view(),
            self._pipeline_health_view(),
            self._cost_tracking_view(),
            self._sla_summary_view(),
        ]
        return "\n\n".join(sections)

    def _schema_setup(self) -> str:
        return f"""-- ═══════════════════════════════════════════════════════════════
-- OBSERVABILITY SCHEMA SETUP
-- ═══════════════════════════════════════════════════════════════
CREATE SCHEMA IF NOT EXISTS {self.fqn};
USE SCHEMA {self.fqn};
"""

    def _execution_history_table(self) -> str:
        return f"""-- Pipeline Execution History (append after each run)
CREATE TABLE IF NOT EXISTS {self.fqn}.pipeline_executions (
  pipeline_name STRING NOT NULL,
  domain STRING,
  layer STRING,
  execution_id STRING,
  status STRING,  -- SUCCESS, FAILED, TIMEOUT, SKIPPED
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  duration_seconds INT,
  rows_read BIGINT,
  rows_written BIGINT,
  bytes_written BIGINT,
  error_message STRING,
  notebook_url STRING,
  cluster_id STRING,
  dbu_cost DECIMAL(10,2),
  _recorded_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(started_at))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality.pipeline' = 'observability'
);
"""

    def _freshness_view(self) -> str:
        return f"""-- Freshness Monitor View (check table staleness)
CREATE OR REPLACE VIEW {self.fqn}.v_freshness_status AS
WITH table_freshness AS (
  SELECT
    pipeline_name,
    domain,
    layer,
    MAX(completed_at) as last_success,
    TIMESTAMPDIFF(MINUTE, MAX(completed_at), current_timestamp()) as staleness_minutes
  FROM {self.fqn}.pipeline_executions
  WHERE status = 'SUCCESS'
  GROUP BY pipeline_name, domain, layer
)
SELECT *,
  CASE
    WHEN staleness_minutes > 1440 THEN 'CRITICAL'  -- > 24h
    WHEN staleness_minutes > 360 THEN 'WARNING'    -- > 6h
    WHEN staleness_minutes > 60 THEN 'ATTENTION'   -- > 1h
    ELSE 'HEALTHY'
  END as health_status
FROM table_freshness;
"""

    def _volume_view(self) -> str:
        return f"""-- Volume Anomaly View (detect unexpected row count changes)
CREATE OR REPLACE VIEW {self.fqn}.v_volume_anomalies AS
SELECT
  pipeline_name,
  date(started_at) as run_date,
  rows_written,
  AVG(rows_written) OVER (
    PARTITION BY pipeline_name
    ORDER BY date(started_at)
    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
  ) as avg_7d,
  CASE
    WHEN rows_written = 0 THEN 'ZERO_ROWS'
    WHEN rows_written < AVG(rows_written) OVER (
      PARTITION BY pipeline_name ORDER BY date(started_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) * 0.3 THEN 'CRITICAL_LOW'
    WHEN rows_written < AVG(rows_written) OVER (
      PARTITION BY pipeline_name ORDER BY date(started_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) * 0.5 THEN 'LOW'
    WHEN rows_written > AVG(rows_written) OVER (
      PARTITION BY pipeline_name ORDER BY date(started_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) * 3.0 THEN 'HIGH'
    ELSE 'NORMAL'
  END as volume_status
FROM {self.fqn}.pipeline_executions
WHERE status = 'SUCCESS'
  AND started_at >= current_date() - INTERVAL 30 DAYS;
"""

    def _dq_scores_view(self) -> str:
        return f"""-- Data Quality Scores (daily quality trending)
CREATE OR REPLACE VIEW {self.fqn}.v_dq_scores AS
SELECT
  pipeline_name,
  domain,
  date(started_at) as run_date,
  ROUND(AVG(CASE WHEN rows_written > 0 THEN 1.0 ELSE 0.0 END) * 100, 1) as completeness_pct,
  COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successes,
  COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failures,
  ROUND(
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1
  ) as success_rate_pct
FROM {self.fqn}.pipeline_executions
WHERE started_at >= current_date() - INTERVAL 30 DAYS
GROUP BY pipeline_name, domain, date(started_at);
"""

    def _pipeline_health_view(self) -> str:
        return f"""-- Pipeline Health Summary (executive dashboard)
CREATE OR REPLACE VIEW {self.fqn}.v_pipeline_health AS
SELECT
  domain,
  COUNT(DISTINCT pipeline_name) as total_pipelines,
  COUNT(CASE WHEN latest_status = 'SUCCESS' THEN 1 END) as healthy,
  COUNT(CASE WHEN latest_status = 'FAILED' THEN 1 END) as failing,
  ROUND(
    COUNT(CASE WHEN latest_status = 'SUCCESS' THEN 1 END) * 100.0 /
    NULLIF(COUNT(DISTINCT pipeline_name), 0), 1
  ) as health_pct
FROM (
  SELECT pipeline_name, domain,
    FIRST_VALUE(status) OVER (PARTITION BY pipeline_name ORDER BY started_at DESC) as latest_status
  FROM {self.fqn}.pipeline_executions
  WHERE started_at >= current_date() - INTERVAL 7 DAYS
)
GROUP BY domain;
"""

    def _cost_tracking_view(self) -> str:
        return f"""-- Cost Tracking (DBU consumption by pipeline)
CREATE OR REPLACE VIEW {self.fqn}.v_cost_by_pipeline AS
SELECT
  pipeline_name,
  domain,
  layer,
  date(started_at) as run_date,
  COUNT(*) as executions,
  SUM(dbu_cost) as total_dbu_cost,
  AVG(duration_seconds) as avg_duration_sec,
  SUM(rows_written) as total_rows
FROM {self.fqn}.pipeline_executions
WHERE started_at >= current_date() - INTERVAL 30 DAYS
GROUP BY pipeline_name, domain, layer, date(started_at)
ORDER BY total_dbu_cost DESC;
"""

    def _sla_summary_view(self) -> str:
        return f"""-- SLA Compliance Summary
CREATE OR REPLACE VIEW {self.fqn}.v_sla_compliance AS
SELECT
  domain,
  COUNT(*) as total_runs,
  COUNT(CASE WHEN duration_seconds <= 3600 THEN 1 END) as within_1h_sla,
  ROUND(COUNT(CASE WHEN duration_seconds <= 3600 THEN 1 END) * 100.0 / COUNT(*), 1) as sla_compliance_pct,
  PERCENTILE(duration_seconds, 0.5) as p50_duration,
  PERCENTILE(duration_seconds, 0.95) as p95_duration,
  PERCENTILE(duration_seconds, 0.99) as p99_duration
FROM {self.fqn}.pipeline_executions
WHERE started_at >= current_date() - INTERVAL 30 DAYS
  AND status = 'SUCCESS'
GROUP BY domain;
"""


class IdempotentPatterns:
    """Idempotent pipeline design patterns for safe re-runs."""

    @staticmethod
    def generate_merge_pattern(target_table: str, merge_keys: list, partition_col: str = None) -> str:
        """Generate idempotent merge/upsert pattern."""
        key_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        part_filter = ""
        if partition_col:
            part_filter = f"\n    AND target.{partition_col} IN (SELECT DISTINCT {partition_col} FROM source_deduped)"
        return f"""-- Idempotent Merge Pattern (safe to re-run)
CREATE OR REPLACE TEMP VIEW source_deduped AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY {", ".join(merge_keys)} ORDER BY _ingested_at DESC) as _rn
  FROM new_data
) WHERE _rn = 1;

MERGE INTO {target_table} AS target
USING source_deduped AS source
ON {key_condition}{part_filter}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
"""

    @staticmethod
    def generate_overwrite_partition_pattern(target_table: str, partition_col: str) -> str:
        """Generate idempotent partition overwrite pattern."""
        return f"""-- Idempotent Partition Overwrite (replaces only affected partitions)
-- SET spark.sql.sources.partitionOverwriteMode = dynamic;
-- df.write.format("delta").mode("overwrite")
--   .option("replaceWhere", "{partition_col} = '<value>'")
--   .saveAsTable("{target_table}")
"""

    @staticmethod
    def generate_checkpoint_pattern(pipeline_name: str, catalog: str = "production") -> str:
        """Generate watermark/checkpoint pattern for incremental loads."""
        return (
            f"-- Checkpoint Pattern for {pipeline_name}\n"
            f"CREATE TABLE IF NOT EXISTS {catalog}.observability.pipeline_checkpoints (\n"
            f"  pipeline_name STRING, checkpoint_key STRING, checkpoint_value STRING,\n"
            f"  updated_at TIMESTAMP DEFAULT current_timestamp()\n"
            f") USING DELTA;\n\n"
            f"-- Usage in notebook:\n"
            f"-- last_cp = spark.sql('SELECT checkpoint_value FROM {catalog}.observability.pipeline_checkpoints "
            f"WHERE pipeline_name = \'{pipeline_name}\' AND checkpoint_key = \'last_processed\'').collect()\n"
            f"-- watermark = last_cp[0][0] if last_cp else '1900-01-01'\n"
            f"-- df = spark.table('source').filter(f'updated_at > \'{{watermark}}\'')"
        )


class PartitionStrategy:
    """Recommends partitioning and Z-ordering strategies."""

    @staticmethod
    def recommend(table_size_gb: float, query_patterns: list[str],
                  cardinality: dict[str, int]) -> dict:
        """Recommend partition and Z-order strategy."""
        recommendation = {
            "partition_by": [],
            "z_order_by": [],
            "reasoning": [],
        }

        # Partition: only for large tables, low-cardinality columns
        if table_size_gb > 100:
            for col, card in sorted(cardinality.items(), key=lambda x: x[1]):
                if card < 1000 and any(col in q for q in query_patterns):
                    recommendation["partition_by"].append(col)
                    recommendation["reasoning"].append(
                        f"Partition by {col} (cardinality={card}, table={table_size_gb}GB)")
                    break  # Usually 1 partition column

        # Z-order: high-cardinality columns used in filters
        for col, card in sorted(cardinality.items(), key=lambda x: -x[1]):
            if card > 1000 and any(col in q for q in query_patterns):
                recommendation["z_order_by"].append(col)
                if len(recommendation["z_order_by"]) >= 3:
                    break

        if not recommendation["z_order_by"]:
            recommendation["reasoning"].append("No Z-ORDER columns identified; add frequently filtered high-cardinality columns")

        return recommendation
