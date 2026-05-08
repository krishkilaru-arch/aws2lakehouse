"""
Config-Driven Pipeline Framework - The Migration Factory Engine.

Turns YAML/JSON pipeline specs into production Databricks artifacts:
notebook code, job YAML, DQ SQL, governance SQL, tests, monitoring.

Usage:
    from aws2lakehouse.factory import PipelineSpec, PipelineFactory
    spec = PipelineSpec.from_yaml("pipelines/loan_ingestion.yaml")
    factory = PipelineFactory(catalog="production", environment="prod")
    artifacts = factory.generate(spec)
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import yaml


class SourceType(Enum):
    KAFKA = "kafka"
    AUTO_LOADER = "auto_loader"
    DELTA_SHARE = "delta_share"
    JDBC = "jdbc"
    MONGODB = "mongodb"
    SNOWFLAKE = "snowflake"
    DELTA_TABLE = "delta_table"
    API = "api"


class TargetMode(Enum):
    STREAMING = "streaming"
    BATCH = "batch"
    MERGE = "merge"


class QualityAction(Enum):
    WARN = "warn"
    DROP = "drop"
    QUARANTINE = "quarantine"
    FAIL = "fail"


class Classification(Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    MNPI = "mnpi"


@dataclass
class SourceConfig:
    type: SourceType
    config: dict[str, Any] = field(default_factory=dict)
    schema_path: Optional[str] = None
    watermark_column: Optional[str] = None
    watermark_delay: str = "10 minutes"


@dataclass
class TargetConfig:
    catalog: str = "production"
    schema: str = "bronze"
    table: str = ""
    format: str = "delta"
    mode: TargetMode = TargetMode.BATCH
    partition_by: list[str] = field(default_factory=list)
    z_order_by: list[str] = field(default_factory=list)
    merge_keys: list[str] = field(default_factory=list)
    scd_type: int = 1
    soft_delete: bool = False


@dataclass
class QualityRule:
    name: str
    condition: str
    action: QualityAction = QualityAction.WARN
    threshold: float = 1.0


@dataclass
class GovernanceConfig:
    classification: Classification = Classification.INTERNAL
    embargo_hours: int = 0
    mnpi_columns: list[str] = field(default_factory=list)
    row_filter: Optional[str] = None
    column_masks: dict[str, str] = field(default_factory=dict)
    owner_group: Optional[str] = None


@dataclass
class ComputeConfig:
    type: str = "serverless"
    photon: bool = True
    node_type: Optional[str] = None
    min_workers: int = 1
    max_workers: int = 4


@dataclass
class ScheduleConfig:
    cron: Optional[str] = None
    timezone: str = "UTC"
    max_attempts: int = 3
    backoff_seconds: int = 60
    timeout_minutes: int = 60
    sla_minutes: Optional[int] = None


@dataclass
class AlertConfig:
    on_failure: list[str] = field(default_factory=list)
    on_sla_breach: list[str] = field(default_factory=list)
    on_success: list[str] = field(default_factory=list)


@dataclass
class PipelineSpec:
    """Complete pipeline specification - one YAML = one pipeline."""
    name: str
    domain: str = "default"
    owner: str = ""
    description: str = ""
    source: SourceConfig = field(default_factory=lambda: SourceConfig(type=SourceType.AUTO_LOADER))
    target: TargetConfig = field(default_factory=TargetConfig)
    transform_sql: Optional[str] = None
    transform_notebook: Optional[str] = None
    quality: list[QualityRule] = field(default_factory=list)
    governance: GovernanceConfig = field(default_factory=GovernanceConfig)
    compute: ComputeConfig = field(default_factory=ComputeConfig)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    alerting: AlertConfig = field(default_factory=AlertConfig)
    tags: dict[str, str] = field(default_factory=dict)
    layer: str = "bronze"
    depends_on: list[str] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: str) -> "PipelineSpec":
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineSpec":
        if "name" not in data or not data["name"]:
            raise ValueError(
                "PipelineSpec requires a 'name' field. "
                f"Got keys: {list(data.keys())}"
            )
        spec = cls(name=data["name"], domain=data.get("domain", "default"),
                   owner=data.get("owner", ""), description=data.get("description", ""),
                   layer=data.get("layer", "bronze"), depends_on=data.get("depends_on", []),
                   tags=data.get("tags", {}))
        src = data.get("source", {})
        spec.source = SourceConfig(type=SourceType(src.get("type", "auto_loader")),
                                   config=src.get("config", {}))
        tgt = data.get("target", {})
        spec.target = TargetConfig(catalog=tgt.get("catalog", "production"),
                                   schema=tgt.get("schema", "bronze"),
                                   table=tgt.get("table", data["name"]),
                                   mode=TargetMode(tgt.get("mode", "batch")),
                                   partition_by=tgt.get("partition_by", []),
                                   z_order_by=tgt.get("z_order_by", []),
                                   merge_keys=tgt.get("merge_keys", []),
                                   scd_type=tgt.get("scd_type", 1),
                                   soft_delete=tgt.get("soft_delete", False))
        for rule in data.get("quality", []):
            spec.quality.append(QualityRule(name=rule["name"], condition=rule["condition"],
                                           action=QualityAction(rule.get("action", "warn")),
                                           threshold=rule.get("threshold", 1.0)))
        gov = data.get("governance", {})
        spec.governance = GovernanceConfig(
            classification=Classification(gov.get("classification", "internal")),
            embargo_hours=gov.get("embargo_hours", 0),
            mnpi_columns=gov.get("mnpi_columns", []),
            row_filter=gov.get("row_filter"),
            column_masks=gov.get("column_masks", {}),
            owner_group=gov.get("owner_group"))
        comp = data.get("compute", {})
        spec.compute = ComputeConfig(type=comp.get("type", "serverless"), photon=comp.get("photon", True))
        sched = data.get("schedule", {})
        retry = data.get("retry", {})
        spec.schedule = ScheduleConfig(cron=sched.get("cron"), timezone=sched.get("timezone", "UTC"),
                                       max_attempts=retry.get("max_attempts", 3),
                                       backoff_seconds=retry.get("backoff_seconds", 60),
                                       sla_minutes=sched.get("sla_minutes"))
        alert = data.get("alerting", {})
        spec.alerting = AlertConfig(on_failure=alert.get("on_failure", []),
                                    on_sla_breach=alert.get("on_sla_breach", []))
        spec.transform_sql = data.get("transform_sql")
        return spec

    def to_yaml(self) -> str:
        data = {"name": self.name, "domain": self.domain, "owner": self.owner, "layer": self.layer,
                "source": {"type": self.source.type.value, "config": self.source.config},
                "target": {"catalog": self.target.catalog, "schema": self.target.schema,
                           "table": self.target.table, "mode": self.target.mode.value},
                "quality": [{"name": r.name, "condition": r.condition, "action": r.action.value} for r in self.quality],
                "governance": {"classification": self.governance.classification.value},
                "schedule": {"cron": self.schedule.cron}, "tags": self.tags}
        return yaml.dump(data, default_flow_style=False, sort_keys=False)


@dataclass
class PipelineArtifacts:
    """Generated artifacts from a pipeline spec."""
    spec: PipelineSpec
    notebook_code: str = ""
    job_yaml: str = ""
    dq_sql: str = ""
    governance_sql: str = ""
    test_code: str = ""
    monitoring_sql: str = ""


class PipelineFactory:
    """
    The Migration Factory Engine.

    Turns YAML pipeline specs into production Databricks artifacts:
    - Notebook code (Bronze/Silver/Gold patterns)
    - Job YAML (DAB-compatible)
    - Data quality SQL (SDP expectations)
    - Governance SQL (tags, column masks, row filters)
    - Test code (pytest validation)
    - Monitoring SQL (freshness, volume anomalies)
    """

    def __init__(self, catalog: str = "production", environment: str = "prod"):
        self.catalog = catalog
        self.environment = environment

    def generate(self, spec: PipelineSpec) -> PipelineArtifacts:
        """Generate all artifacts from a pipeline spec."""
        artifacts = PipelineArtifacts(spec=spec)
        artifacts.notebook_code = self._gen_notebook(spec)
        artifacts.job_yaml = self._gen_job_yaml(spec)
        artifacts.dq_sql = self._gen_dq_sql(spec)
        artifacts.governance_sql = self._gen_governance_sql(spec)
        artifacts.test_code = self._gen_tests(spec)
        artifacts.monitoring_sql = self._gen_monitoring(spec)
        return artifacts

    def generate_batch(self, specs_dir: str) -> list[PipelineArtifacts]:
        """Generate artifacts for all YAML specs in a directory."""
        results = []
        for f in sorted(Path(specs_dir).glob("*.y*ml")):
            results.append(self.generate(PipelineSpec.from_yaml(str(f))))
        return results

    def _get_source_code(self, spec: PipelineSpec) -> str:
        """Generate source-specific reading code."""
        cfg = spec.source.config
        src_type = spec.source.type

        if src_type == SourceType.KAFKA:
            scope = cfg.get("secret_scope", "kafka")
            topic = cfg.get("topic", "events")
            offsets = cfg.get("starting_offsets", "latest")
            max_off = cfg.get("max_offsets", "100000")
            lines = [
                "# Read from Kafka",
                "raw_df = (",
                "    spark.readStream",
                "    .format(\"kafka\")",
                f"    .option(\"kafka.bootstrap.servers\", dbutils.secrets.get(\"{scope}\", \"bootstrap_servers\"))",
                f"    .option(\"subscribe\", \"{topic}\")",
                f"    .option(\"startingOffsets\", \"{offsets}\")",
                f"    .option(\"maxOffsetsPerTrigger\", \"{max_off}\")",
                "    .load()",
                ")",
                "df = (",
                "    raw_df",
                "    .selectExpr(\"CAST(key AS STRING)\", \"CAST(value AS STRING)\", \"topic\", \"partition\", \"offset\", \"timestamp\")",
                "    .withColumn(\"_ingested_at\", F.current_timestamp())",
                "    .withColumn(\"_kafka_offset\", F.col(\"offset\"))",
                ")",
            ]
            return "\n".join(lines)

        elif src_type == SourceType.AUTO_LOADER:
            fmt = cfg.get("format", "json")
            path = cfg.get("path", f"/Volumes/{spec.target.catalog}/raw/{spec.target.table}/")
            schema_loc = f"/Volumes/{spec.target.catalog}/{spec.target.schema}/_schemas/{spec.target.table}"
            lines = [
                f"# Auto Loader ({fmt})",
                "df = (",
                "    spark.readStream",
                "    .format(\"cloudFiles\")",
                f"    .option(\"cloudFiles.format\", \"{fmt}\")",
                f"    .option(\"cloudFiles.schemaLocation\", \"{schema_loc}\")",
                "    .option(\"cloudFiles.inferColumnTypes\", \"true\")",
                "    .option(\"cloudFiles.schemaEvolutionMode\", \"addNewColumns\")",
                "    .option(\"rescuedDataColumn\", \"_rescued_data\")",
                f"    .load(\"{path}\")",
                "    .withColumn(\"_ingested_at\", F.current_timestamp())",
                "    .withColumn(\"_source_file\", F.input_file_name())",
                ")",
            ]
            return "\n".join(lines)

        elif src_type == SourceType.DELTA_SHARE:
            share = cfg.get("share_name", "vendor")
            schema = cfg.get("schema", "data")
            table = cfg.get("table", spec.name)
            lines = [
                "# Delta Share + CDF",
                "df = (",
                "    spark.readStream",
                "    .format(\"deltaSharing\")",
                "    .option(\"readChangeFeed\", \"true\")",
                f"    .table(\"{share}.{schema}.{table}\")",
                "    .withColumn(\"_ingested_at\", F.current_timestamp())",
                ")",
            ]
            return "\n".join(lines)

        elif src_type == SourceType.JDBC:
            scope = cfg.get("secret_scope", "jdbc")
            src_table = cfg.get("source_table", spec.name)
            part_col = cfg.get("partition_column", "id")
            lines = [
                f"# JDBC: {src_table}",
                "df = (",
                "    spark.read.format(\"jdbc\")",
                f"    .option(\"url\", dbutils.secrets.get(\"{scope}\", \"url\"))",
                f"    .option(\"user\", dbutils.secrets.get(\"{scope}\", \"username\"))",
                f"    .option(\"password\", dbutils.secrets.get(\"{scope}\", \"password\"))",
                f"    .option(\"dbtable\", \"{src_table}\")",
                f"    .option(\"partitionColumn\", \"{part_col}\")",
                f"    .option(\"numPartitions\", \"{cfg.get('num_partitions', '20')}\")",
                f"    .option(\"fetchsize\", \"{cfg.get('fetchsize', '10000')}\")",
                "    .load()",
                "    .withColumn(\"_ingested_at\", F.current_timestamp())",
                ")",
            ]
            return "\n".join(lines)

        elif src_type == SourceType.MONGODB:
            scope = cfg.get("secret_scope", "mongo")
            db = cfg.get("database", "prod")
            coll = cfg.get("collection", spec.name)
            lines = [
                f"# MongoDB: {db}.{coll}",
                "df = (",
                "    spark.readStream.format(\"mongodb\")",
                f"    .option(\"connection.uri\", dbutils.secrets.get(\"{scope}\", \"uri\"))",
                f"    .option(\"database\", \"{db}\")",
                f"    .option(\"collection\", \"{coll}\")",
                "    .option(\"change.stream.publish.full.document.only\", \"true\")",
                "    .load()",
                "    .withColumn(\"_ingested_at\", F.current_timestamp())",
                ")",
            ]
            return "\n".join(lines)

        elif src_type == SourceType.DELTA_TABLE:
            src_table = cfg.get("source_table", "")
            if spec.target.mode == TargetMode.STREAMING:
                return f"df = spark.readStream.table(\"{src_table}\")"
            return f"df = spark.table(\"{src_table}\")"

        return "# TODO: Implement source\ndf = spark.createDataFrame([], \"placeholder STRING\")"

    def _gen_notebook(self, spec: PipelineSpec) -> str:
        """Generate complete notebook code."""
        fqn = f"{spec.target.catalog}.{spec.target.schema}.{spec.target.table}"
        sections = []

        # Header
        sections.append("# Databricks notebook source")
        sections.append("# MAGIC %md")
        sections.append(f"# MAGIC # {spec.name} - {spec.layer.title()} Layer")
        sections.append(f"# MAGIC **Domain:** {spec.domain} | **Owner:** {spec.owner}")
        sections.append(f"# MAGIC **Target:** `{fqn}` | **Mode:** {spec.target.mode.value}")
        sections.append(f"# MAGIC **Classification:** {spec.governance.classification.value}")
        sections.append("")
        sections.append("# COMMAND ----------")
        sections.append("")
        sections.append("from pyspark.sql import functions as F")
        sections.append("from datetime import datetime")
        sections.append("import os")
        sections.append("")
        sections.append(f"CATALOG = os.getenv(\"CATALOG\", \"{spec.target.catalog}\")")
        sections.append(f"SCHEMA = os.getenv(\"SCHEMA\", \"{spec.target.schema}\")")
        sections.append(f"TABLE = os.getenv(\"TABLE\", \"{spec.target.table}\")")
        sections.append("TARGET_TABLE = f\"{CATALOG}.{SCHEMA}.{TABLE}\"")
        sections.append("")
        sections.append("# COMMAND ----------")
        sections.append("")

        # Source
        sections.append(self._get_source_code(spec))
        sections.append("")
        sections.append("# COMMAND ----------")
        sections.append("")

        # Transform
        if spec.transform_sql:
            sections.append("# SQL Transformation")
            sections.append("df.createOrReplaceTempView(\"source_data\")")
            sections.append(f"df = spark.sql(\"\"\"{spec.transform_sql}\"\"\")")
            sections.append("")
            sections.append("# COMMAND ----------")
            sections.append("")
        elif spec.layer == "silver":
            sections.append("# Silver: Dedup + Clean")
            if spec.target.merge_keys:
                sections.append(f"df = df.dropDuplicates({spec.target.merge_keys})")
            sections.append("df = df.withColumn(\"_processed_at\", F.current_timestamp())")
            sections.append("")
            sections.append("# COMMAND ----------")
            sections.append("")

        # Quality
        if spec.quality:
            sections.append("# Data Quality Checks")
            for r in spec.quality:
                if r.action == QualityAction.DROP:
                    sections.append(f"df = df.filter(\"{r.condition}\")  # DQ: {r.name}")
                elif r.action == QualityAction.FAIL:
                    sections.append(f"assert df.filter(\"NOT ({r.condition})\").count() == 0, \"DQ FAIL: {r.name}\"")
                else:
                    sections.append(f"# DQ check ({r.action.value}): {r.name} -> {r.condition}")
            sections.append("")
            sections.append("# COMMAND ----------")
            sections.append("")

        # Sink
        if spec.target.mode == TargetMode.STREAMING:
            cp = f"/Volumes/{spec.target.catalog}/{spec.target.schema}/_checkpoints/{spec.target.table}"
            sections.append("# Write: Streaming append to Delta")
            sections.append("query = (")
            sections.append("    df.writeStream")
            sections.append("    .format(\"delta\")")
            sections.append("    .outputMode(\"append\")")
            sections.append(f"    .option(\"checkpointLocation\", \"{cp}\")")
            sections.append("    .option(\"mergeSchema\", \"true\")")
            sections.append("    .trigger(availableNow=True)")
            sections.append("    .toTable(TARGET_TABLE)")
            sections.append(")")
            sections.append("")
            sections.append("query.awaitTermination()")
        elif spec.target.mode == TargetMode.MERGE:
            keys_cond = " AND ".join([f"t.{k} = s.{k}" for k in spec.target.merge_keys])
            sections.append("# Write: Merge/Upsert")
            sections.append("from delta.tables import DeltaTable")
            sections.append("if spark.catalog.tableExists(TARGET_TABLE):")
            sections.append("    target = DeltaTable.forName(spark, TARGET_TABLE)")
            sections.append(f"    (target.alias(\"t\").merge(df.alias(\"s\"), \"{keys_cond}\")")
            sections.append("        .whenMatchedUpdateAll()")
            sections.append("        .whenNotMatchedInsertAll()")
            sections.append("        .execute())")
            sections.append("else:")
            sections.append("    df.write.format(\"delta\").saveAsTable(TARGET_TABLE)")
        else:
            mode = "overwrite" if spec.layer == "gold" else "append"
            sections.append(f"# Write: Batch {mode}")
            write_line = f"df.write.format(\"delta\").mode(\"{mode}\").option(\"mergeSchema\", \"true\")"
            if spec.target.partition_by:
                parts = ", ".join([f"\"{p}\"" for p in spec.target.partition_by])
                write_line += f".partitionBy({parts})"
            write_line += ".saveAsTable(TARGET_TABLE)"
            sections.append(write_line)

        sections.append("")
        sections.append("# COMMAND ----------")

        # Optimize
        if spec.target.z_order_by:
            z = ", ".join(spec.target.z_order_by)
            sections.append(f"spark.sql(f\"OPTIMIZE {{TARGET_TABLE}} ZORDER BY ({z})\")")

        sections.append(f"print(f\"Pipeline {spec.name} complete -> {{TARGET_TABLE}}\")")

        return "\n".join(sections)

    def _gen_job_yaml(self, spec: PipelineSpec) -> str:
        """Generate DAB-compatible job resource YAML."""
        sched = ""
        if spec.schedule.cron:
            quartz = self._to_quartz(spec.schedule.cron)
            sched = f"\n  schedule:\n    quartz_cron_expression: \"{quartz}\"\n    timezone_id: \"{spec.schedule.timezone}\"\n    pause_status: UNPAUSED"

        return f"""resources:
  jobs:
    {spec.name}:
      name: "[{self.environment}] {spec.domain}/{spec.name}"
      description: "{spec.description or spec.name}"{sched}
      tasks:
        - task_key: {spec.name}
          notebook_task:
            notebook_path: pipelines/{spec.domain}/{spec.name}
            base_parameters:
              CATALOG: "{spec.target.catalog}"
              SCHEMA: "{spec.target.schema}"
              TABLE: "{spec.target.table}"
          max_retries: {spec.schedule.max_attempts - 1}
          timeout_seconds: {spec.schedule.timeout_minutes * 60}
      tags:
        domain: "{spec.domain}"
        layer: "{spec.layer}"
        classification: "{spec.governance.classification.value}"
"""

    def _gen_dq_sql(self, spec: PipelineSpec) -> str:
        """Generate SDP expectations SQL."""
        if not spec.quality:
            return f"-- No quality rules defined for {spec.name}\n-- Add rules in the pipeline spec to auto-generate expectations"
        fqn = f"{spec.target.catalog}.{spec.target.schema}.{spec.target.table}"
        action_map = {"warn": "DROP ROW", "drop": "DROP ROW", "quarantine": "DROP ROW", "fail": "FAIL UPDATE"}
        constraints = []
        for r in spec.quality:
            constraints.append(f"  CONSTRAINT {r.name} EXPECT ({r.condition}) ON VIOLATION {action_map[r.action.value]}")
        return f"-- SDP Expectations for {spec.name}\nCREATE OR REFRESH STREAMING TABLE {fqn} (\n" + ",\n".join(constraints) + "\n);"

    def _gen_governance_sql(self, spec: PipelineSpec) -> str:
        """Generate governance SQL: tags, column masks, row filters.

        F6: Infers column type from mask expression to generate correct function signature.
        """
        fqn = f"{spec.target.catalog}.{spec.target.schema}.{spec.target.table}"
        stmts = [f"-- Governance: {spec.name}"]
        stmts.append(f"ALTER TABLE {fqn} SET TAGS ('data_classification' = '{spec.governance.classification.value}');")
        stmts.append(f"ALTER TABLE {fqn} SET TAGS ('domain' = '{spec.domain}', 'owner' = '{spec.owner}');")

        for col in spec.governance.mnpi_columns:
            stmts.append(f"ALTER TABLE {fqn} ALTER COLUMN {col} SET TAGS ('mnpi' = 'true');")

        for col, mask_expr in spec.governance.column_masks.items():
            fn = f"{spec.target.catalog}.{spec.target.schema}.mask_{spec.target.table}_{col}"
            group = spec.governance.owner_group or "mnpi_approved"
            # F6: Infer type from mask expression
            col_type = self._infer_mask_type(mask_expr)
            stmts.append(f"CREATE OR REPLACE FUNCTION {fn}(val {col_type}) RETURNS {col_type}")
            stmts.append(f"  RETURN CASE WHEN is_account_group_member('{group}') THEN val ELSE {mask_expr} END;")
            stmts.append(f"ALTER TABLE {fqn} ALTER COLUMN {col} SET MASK {fn};")

        if spec.governance.row_filter:
            fn = f"{spec.target.catalog}.{spec.target.schema}.filter_{spec.target.table}"
            group = spec.governance.owner_group or "mnpi_approved"
            stmts.append(f"CREATE OR REPLACE FUNCTION {fn}() RETURNS BOOLEAN")
            stmts.append(f"  RETURN CASE WHEN is_account_group_member('{group}') THEN TRUE ELSE {spec.governance.row_filter} END;")
            stmts.append(f"ALTER TABLE {fqn} SET ROW FILTER {fn} ON ();")

        return "\n".join(stmts)

    def _gen_tests(self, spec: PipelineSpec) -> str:
        """Generate pytest test suite."""
        fqn = f"{spec.target.catalog}.{spec.target.schema}.{spec.target.table}"
        sla = spec.schedule.sla_minutes or 1440
        merge_keys = spec.target.merge_keys or []
        lines = [f"# Tests for {spec.name}",
                 "def test_table_exists(spark):",
                 f"    assert spark.catalog.tableExists(\"{fqn}\")",
                 "",]

        # F5: Only generate null-key test when merge_keys are defined
        if merge_keys:
            lines.extend([
                 "def test_no_null_keys(spark):",
                 f"    df = spark.table(\"{fqn}\")",
                 f"    for key in {merge_keys}:",
                 "        assert df.filter(f\"{key} IS NULL\").count() == 0",
                 "",])

        lines.extend([
                 "def test_freshness(spark):",
                 "    from pyspark.sql.functions import max as smax",
                 "    from datetime import datetime, timedelta",
                 f"    latest = spark.table(\"{fqn}\").agg(smax(\"_ingested_at\")).collect()[0][0]",
                 f"    assert datetime.now() - latest < timedelta(minutes={sla})",
                 ])
        for r in spec.quality:
            lines.append("")
            lines.append(f"def test_dq_{r.name}(spark):")
            lines.append(f"    df = spark.table(\"{fqn}\")")
            lines.append("    total = df.count()")
            lines.append(f"    passing = df.filter(\"{r.condition}\").count()")
            lines.append(f"    assert passing / max(total, 1) >= {r.threshold}")
        return "\n".join(lines)

    def _gen_monitoring(self, spec: PipelineSpec) -> str:
        """Generate monitoring SQL for freshness and volume."""
        fqn = f"{spec.target.catalog}.{spec.target.schema}.{spec.target.table}"
        sla = spec.schedule.sla_minutes or 1440
        return f"""-- Monitoring: {spec.name}
-- Freshness SLA check
SELECT '{spec.name}' as pipeline, max(_ingested_at) as last_ingest,
  current_timestamp() - max(_ingested_at) as staleness,
  CASE WHEN current_timestamp() - max(_ingested_at) > INTERVAL {sla} MINUTES THEN 'BREACH' ELSE 'OK' END as sla_status
FROM {fqn};

-- Volume anomaly detection (7-day rolling average)
SELECT date(_ingested_at) as dt, count(*) as row_count,
  avg(count(*)) OVER (ORDER BY date(_ingested_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) as avg_7d,
  CASE
    WHEN count(*) < avg(count(*)) OVER (ORDER BY date(_ingested_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) * 0.5 THEN 'LOW_VOLUME'
    WHEN count(*) > avg(count(*)) OVER (ORDER BY date(_ingested_at) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) * 2.0 THEN 'HIGH_VOLUME'
    ELSE 'NORMAL'
  END as status
FROM {fqn} GROUP BY 1 ORDER BY 1 DESC LIMIT 14;
"""

    @staticmethod
    def _infer_mask_type(mask_expr: str) -> str:
        """F6: Infer SQL type from mask expression for column mask functions.

        Examples:
            "0.00" → DOUBLE
            "0" → BIGINT
            "'[REDACTED]'" → STRING
            "NULL" → STRING (safe default)
            "CAST(0 AS DECIMAL(10,2))" → DECIMAL(10,2)
        """
        mask = mask_expr.strip()
        if mask.startswith("'") or mask.startswith('"'):
            return "STRING"
        if "DECIMAL" in mask.upper():
            return "DECIMAL(18,2)"
        if "." in mask and mask.replace(".", "").replace("-", "").isdigit():
            return "DOUBLE"
        if mask.replace("-", "").isdigit():
            return "BIGINT"
        if mask.upper() in ("TRUE", "FALSE"):
            return "BOOLEAN"
        # Default to STRING for safety (works with most types via implicit cast)
        return "STRING"

    @staticmethod
    def _to_quartz(cron_expr: str) -> str:
        """Convert 5-field cron to 7-field Quartz.

        Quartz requires exactly one of day-of-month or day-of-week to be '?'.
        Also converts cron DOW values (0-6, Sun=0) to Quartz (1-7, Sun=1).
        F4: Properly handles when both DOW and DOM are specified.
        """
        parts = cron_expr.split()
        if len(parts) == 5:
            minute, hour, dom, month, dow = parts

            # Convert cron DOW (0-6, Sun=0) to Quartz (1-7, Sun=1)
            if dow != "*" and dow != "?":
                # Handle numeric DOW values
                try:
                    dow_int = int(dow)
                    dow = str(dow_int + 1)  # cron 0=Sun -> Quartz 1=Sun
                except ValueError:
                    pass  # Named days (MON, TUE) are the same in Quartz

            # Quartz mutual exclusion: one of DOM/DOW must be "?"
            if dow != "*" and dom != "*":
                # Both specified -- DOW takes precedence, DOM becomes "?"
                dom = "?"
            elif dow != "*":
                # DOW specified, DOM must be "?"
                dom = "?"
            elif dom != "*":
                # DOM specified, DOW must be "?"
                dow = "?"
            else:
                # Both are "*" -- default: use DOM=*, DOW=?
                dow = "?"

            return f"0 {minute} {hour} {dom} {month} {dow} *"
        return cron_expr


__all__ = ["PipelineSpec", "PipelineFactory", "PipelineArtifacts",
           "SourceType", "TargetMode", "QualityAction", "Classification",
           "SourceConfig", "TargetConfig", "QualityRule", "GovernanceConfig",
           "ComputeConfig", "ScheduleConfig", "AlertConfig"]
