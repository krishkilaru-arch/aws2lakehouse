"""
Spec Generator — Auto-generate pipeline YAML specs from inventory data.

This module bridges the gap between Discovery (scanner output) and the Factory
(which consumes YAML specs). Instead of manually writing YAML for 1000 pipelines,
the scanners populate an inventory and this module converts each row into a
PipelineSpec that the factory can process.

Usage:
    from aws2lakehouse.factory.spec_generator import SpecGenerator

    # From a list of inventory dicts (from scanners)
    generator = SpecGenerator(catalog="production")
    specs = generator.from_inventory(pipeline_inventory_list)

    # From the migration_tracking.pipeline_inventory table (in production)
    specs = generator.from_table("production.migration_tracking.pipeline_inventory")

    # Write all specs to YAML files
    generator.write_specs_to_directory(specs, output_dir="specs/")
"""

from pathlib import Path

from aws2lakehouse.factory import (
    AlertConfig,
    Classification,
    ComputeConfig,
    GovernanceConfig,
    PipelineSpec,
    QualityAction,
    QualityRule,
    ScheduleConfig,
    SourceConfig,
    SourceType,
    TargetConfig,
    TargetMode,
)


class SpecGenerator:
    """
    Converts scanner/inventory output into PipelineSpec objects.

    This is the key automation layer that eliminates manual YAML writing.
    Scanners populate metadata → SpecGenerator creates specs → Factory generates code.
    """

    def __init__(self, catalog: str = "production",
                 default_schema_pattern: str = "{domain}_{layer}"):
        self.catalog = catalog
        self.schema_pattern = default_schema_pattern

    def from_inventory(self, pipelines: list[dict]) -> list[PipelineSpec]:
        """
        Convert a list of inventory dicts into PipelineSpec objects.

        Expected dict keys (from scanner output):
            name, domain, source_type, source_config, schedule, sla_minutes,
            business_impact, owner, mnpi_columns, pii_columns, classification,
            tables_read, tables_written, latency_class, complexity_category
        """
        specs = []
        for p in pipelines:
            spec = self._build_spec(p)
            specs.append(spec)
        return specs

    def from_table_sql(self, table_fqn: str) -> str:
        """
        Generate SQL to read pipeline_inventory table and prepare for spec generation.
        Run this in a notebook, then pass results to from_inventory().
        """
        return f"""
-- Read pipeline inventory for spec generation
SELECT
    pipeline_name as name,
    domain,
    source_system as source_type,
    complexity_category,
    latency_class,
    business_impact,
    mnpi_classification as classification,
    sla_minutes,
    owner,
    dependencies
FROM {table_fqn}
WHERE migration_status = 'not_started'
ORDER BY wave_number, complexity_score
"""

    def from_spark_table(self, spark, table_fqn: str) -> "list[PipelineSpec]":
        """S2: Execute the inventory query and convert results to PipelineSpecs.

        Usage (in a Databricks notebook):
            generator = SpecGenerator(catalog="production")
            specs = generator.from_spark_table(spark, "prod.migration_tracking.pipeline_inventory")
        """
        sql = self.from_table_sql(table_fqn)
        df = spark.sql(sql)
        rows = [row.asDict() for row in df.collect()]
        return self.from_inventory(rows)

    def write_specs_to_directory(self, specs: list[PipelineSpec], output_dir: str):
        """Write all specs to YAML files organized by domain."""
        output = Path(output_dir)
        for spec in specs:
            domain_dir = output / spec.domain
            domain_dir.mkdir(parents=True, exist_ok=True)
            yaml_path = domain_dir / f"{spec.name}.yaml"
            yaml_path.write_text(spec.to_yaml(), encoding="utf-8")
        return len(specs)

    def _build_spec(self, p: dict) -> PipelineSpec:
        """Build a PipelineSpec from an inventory dict."""

        # Source configuration
        source_type = self._map_source_type(p.get("source_type", "auto_loader"))
        source_config = p.get("source_config", {})

        # Target configuration
        domain = p.get("domain", "default")
        layer = p.get("layer", "bronze")
        schema = self.schema_pattern.format(domain=domain, layer=layer)

        # Determine write mode from latency
        latency = p.get("latency_class", self._infer_latency(p.get("schedule", "0 6 * * *")))
        if latency in ("streaming", "nrt"):
            mode = TargetMode.STREAMING
        elif p.get("merge_keys"):
            mode = TargetMode.MERGE
        else:
            mode = TargetMode.BATCH

        # Classification
        classification = self._map_classification(p.get("classification", "internal"))

        # MNPI/PII columns
        mnpi_columns = p.get("mnpi_columns", [])
        pii_columns = p.get("pii_columns", [])
        all_sensitive = mnpi_columns + pii_columns

        # Column masks (auto-generate from sensitive columns)
        column_masks = {}
        for col in mnpi_columns:
            column_masks[col] = "0.00"  # Default numeric mask
        for col in pii_columns:
            column_masks[col] = "'[REDACTED]'"  # Default string mask

        # Quality rules (auto-generate basic ones)
        quality_rules = self._generate_default_quality_rules(p)

        # Compute sizing (from EMR metadata if available)
        compute_type = "serverless"
        if p.get("num_executors", 0) > 10:
            compute_type = "job_cluster"

        # Schedule
        schedule = p.get("schedule")
        cron = None if schedule in ("continuous", "triggered") else schedule

        # Build the spec
        spec = PipelineSpec(
            name=p["name"],
            domain=domain,
            owner=p.get("owner", ""),
            description=f"Migrated from AWS: {p.get('source_type', 'unknown')} -> {schema}",
            layer=layer,
            depends_on=p.get("depends_on", []),
            tags={
                "domain": domain,
                "complexity": p.get("complexity_category", "medium"),
                "business_impact": p.get("business_impact", "medium"),
                "migrated_from": p.get("emr_cluster", "aws"),
            }
        )

        spec.source = SourceConfig(type=source_type, config=source_config)
        spec.target = TargetConfig(
            catalog=self.catalog,
            schema=schema,
            table=p["name"],
            mode=mode,
            partition_by=p.get("partition_by", []),
            z_order_by=p.get("z_order_by", []),
            merge_keys=p.get("merge_keys", []),
        )
        spec.quality = quality_rules
        spec.governance = GovernanceConfig(
            classification=classification,
            embargo_hours=p.get("embargo_hours", 24 if classification == Classification.MNPI else 0),
            mnpi_columns=all_sensitive,
            column_masks=column_masks,
            row_filter=p.get("row_filter"),
            owner_group=p.get("owner_group", f"{domain}_team"),
        )
        spec.compute = ComputeConfig(type=compute_type, photon=True)
        spec.schedule = ScheduleConfig(
            cron=cron,
            timezone=p.get("timezone", "America/New_York"),
            sla_minutes=p.get("sla_minutes"),
        )
        spec.alerting = AlertConfig(
            on_failure=[f"slack:#data-alerts-{domain}"],
            on_sla_breach=[f"pagerduty:{domain}-oncall"] if p.get("business_impact") == "critical" else [],
        )

        return spec

    def _map_source_type(self, source_type: str) -> SourceType:
        mapping = {
            "kafka": SourceType.KAFKA,
            "auto_loader": SourceType.AUTO_LOADER,
            "s3": SourceType.AUTO_LOADER,
            "delta_share": SourceType.DELTA_SHARE,
            "jdbc": SourceType.JDBC,
            "postgresql": SourceType.JDBC,
            "mysql": SourceType.JDBC,
            "oracle": SourceType.JDBC,
            "mongodb": SourceType.MONGODB,
            "snowflake": SourceType.SNOWFLAKE,
            "delta_table": SourceType.DELTA_TABLE,
        }
        return mapping.get(source_type.lower(), SourceType.AUTO_LOADER)

    def _map_classification(self, classification: str) -> Classification:
        mapping = {
            "public": Classification.PUBLIC,
            "internal": Classification.INTERNAL,
            "confidential": Classification.CONFIDENTIAL,
            "mnpi": Classification.MNPI,
        }
        return mapping.get(classification.lower(), Classification.INTERNAL)

    def _infer_latency(self, schedule: str) -> str:
        import re
        if schedule == "continuous":
            return "streaming"
        # Match */1 through */5 (sub-5-minute) but not */10, */15, etc.
        if re.search(r"\*/[1-5]\b", schedule):
            return "nrt"
        return "batch"

    def _generate_default_quality_rules(self, p: dict) -> list:
        """Auto-generate basic quality rules from metadata.

        S1: Accepts optional 'pk_column' in inventory dict to override heuristic.
        """
        rules = []

        # Non-null primary key: use explicit pk_column if provided, else heuristic
        pk_col = p.get("pk_column")
        if not pk_col:
            # Heuristic: try common patterns
            name = p["name"]
            candidates = [
                f"{name}_id",                           # exact: trade_events_id
                f"{name.rstrip('s')}_id",               # singular: trade_event_id
                f"{name.split('_')[0]}_id",             # first word: trade_id
                "id",                                    # generic fallback
            ]
            pk_col = candidates[0]  # Use first candidate as default

        rules.append(QualityRule(
            name=f"valid_{pk_col}",
            condition=f"{pk_col} IS NOT NULL",
            action=QualityAction.FAIL,
            threshold=1.0
        ))

        # Freshness check for streaming/NRT
        if p.get("latency_class") in ("streaming", "nrt"):
            rules.append(QualityRule(
                name="data_freshness",
                condition="event_timestamp >= current_timestamp() - INTERVAL 1 HOUR",
                action=QualityAction.WARN,
                threshold=0.95
            ))

        return rules
