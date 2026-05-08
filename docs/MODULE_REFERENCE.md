# Module Reference

## aws2lakehouse.factory

The core migration engine. Turns YAML specs into Databricks artifacts.

### PipelineSpec
```python
PipelineSpec.from_yaml(path: str) -> PipelineSpec      # Load from YAML file
PipelineSpec.from_dict(data: dict) -> PipelineSpec     # Load from dictionary
spec.to_yaml() -> str                                  # Serialize back to YAML
```

### PipelineFactory
```python
factory = PipelineFactory(catalog="production", environment="prod")
artifacts = factory.generate(spec)           # Single pipeline
artifacts_list = factory.generate_batch("pipelines/")  # Entire directory
```

---

## aws2lakehouse.discovery

### PipelineInventory
```python
inventory = PipelineInventory(regions=["us-east-1"])
inventory.scan_emr_clusters(region)          # Scan EMR via boto3
inventory.scan_glue_jobs(region)             # Scan Glue via boto3
inventory.scan_step_functions(region)        # Scan Step Functions via boto3
report = inventory.generate_assessment_report()
```

### ComplexityAnalyzer
```python
analyzer = ComplexityAnalyzer()
score = analyzer.analyze_code(source_code, metadata_dict)
# Returns: .overall_score, .category, .recommended_approach, .estimated_hours
#          .has_udfs, .has_streaming, .uses_hive_metastore
```

### WavePlanner
```python
planner = WavePlanner(inventory)
waves = planner.generate_waves(strategy="risk_first", max_per_wave=50)
```

### LatencyClassifier
```python
LatencyClassifier.classify(schedule, processing_time, trigger_type)
# Returns: "batch" | "nrt" | "streaming"
```

### Scanners
```python
AirflowScanner(dag_folder=).scan_dag_folder()
AirflowScanner(airflow_url=).scan_airflow_api()
SnowflakeScanner().generate_scan_sql()
MongoDBScanner().generate_scan_script(database)
PostgreSQLScanner().generate_scan_sql()
```

---

## aws2lakehouse.governance

### UnityCatalogSetup
```python
uc = UnityCatalogSetup(org, environments, domains)
uc.generate_setup_sql()     # CREATE CATALOG/SCHEMA
uc.generate_grants_sql()    # GRANT statements
uc.generate_volume_sql(buckets)  # CREATE VOLUME
```

### MNPIController
```python
mnpi = MNPIController(catalog)
mnpi.generate_mnpi_tags_sql(configs)         # ALTER TABLE SET TAGS
mnpi.generate_column_mask_sql(configs)       # CREATE FUNCTION + ALTER COLUMN SET MASK
mnpi.generate_dynamic_views_sql(configs)     # CREATE VIEW with RBAC
mnpi.generate_row_filter_sql(table)          # Row-level security
mnpi.generate_audit_table_sql()              # Audit logging table
```

---

## aws2lakehouse.orchestration

### StepFunctionConverter
```python
converter = StepFunctionConverter(target_notebook_base="/Workspace/migrated/")
job = converter.convert(state_machine_json, name, schedule_expression)
# job.tasks, job.schedule, job.warnings
```

### AirflowConverter
```python
converter = AirflowConverter(target_notebook_base="/Workspace/migrated/")
job = converter.convert_dag_file("path/to/dag.py")
job = converter.convert_dag_source(python_source_code)
# job.tasks, job.schedule, job.warnings, job.to_yaml()
```

---

## aws2lakehouse.compute

### ClusterMapper
```python
mapper = ClusterMapper(photon_enabled=True)
config = mapper.map_emr_cluster(
    instance_type, instance_count,
    workload_type="batch_etl",  # batch_etl | sql_analytics | ml_training | streaming
    has_autoscaling=True, min_instances=4, max_instances=20
)
# config.node_type_id, .num_workers, .compute_type, .autoscale_min/max
# config.to_api_payload()  → Jobs API cluster spec
```

---

## aws2lakehouse.quality

### DQFramework
```python
dq = DQFramework(catalog)
dq.add_standard_rules(table, primary_key, not_null_columns)
dq.generate_expectations_sql(table)  # SDP CONSTRAINT syntax
```

### FreshnessSLAMonitor
```python
monitor = FreshnessSLAMonitor(catalog)
monitor.add_table(table, sla_minutes, timestamp_col, owner)
monitor.generate_monitoring_view_sql()   # Unified freshness view
monitor.generate_alert_query()           # Returns only breaches
```

### VolumeAnomalyDetector
```python
detector = VolumeAnomalyDetector(catalog)
detector.add_table(table, low_threshold=0.5, high_threshold=2.0)
detector.generate_anomaly_view_sql()     # Rolling avg detection
detector.generate_alert_query()          # Today's anomalies only
detector.generate_history_table_sql()    # Persist metrics
```

---

## aws2lakehouse.observability

### MonitoringDashboard
```python
dashboard = MonitoringDashboard(catalog, schema)
dashboard.generate_all_sql()  # 7 views: freshness, volume, DQ, health, cost, SLA
```

### IdempotentPatterns
```python
IdempotentPatterns.generate_merge_pattern(target, merge_keys, partition_col)
IdempotentPatterns.generate_overwrite_partition_pattern(target, partition_col)
IdempotentPatterns.generate_checkpoint_pattern(pipeline_name, catalog)
```

### PartitionStrategy
```python
PartitionStrategy.recommend(table_size_gb, query_patterns, cardinality)
# Returns: {"partition_by": [...], "z_order_by": [...], "reasoning": [...]}
```

---

## aws2lakehouse.genai

### PipelineGenerator
```python
gen = PipelineGenerator(model_endpoint="databricks-meta-llama-3-1-70b-instruct")
yaml = gen.from_description("natural language description of pipeline")
```

### DebugAssistant
```python
debugger = DebugAssistant()
diagnosis = debugger.diagnose(error_message, code)
# diagnosis["error_type"], ["likely_cause"], ["suggested_fix"], ["severity"]
```

### LineageExplainer
```python
explainer = LineageExplainer()
explanation = explainer.explain(table_name, lineage_data)
```

---

## aws2lakehouse.roi_calculator

```python
from aws2lakehouse.roi_calculator import ROICalculator, AWSCurrentState, DatabricksProjectedState

current = AWSCurrentState(emr_monthly_cost=..., glue_monthly_cost=..., ...)
projected = DatabricksProjectedState(jobs_dbu_monthly=..., ...)
roi = ROICalculator(current, projected, migration_cost, migration_months)

result = roi.calculate()          # Dict with summary, breakdown, tco, benefits
text = roi.generate_executive_summary()  # Printable executive report
```

---

## aws2lakehouse.cicd

### DABGenerator
```python
dab = DABGenerator(project_name, environments, catalog_prefix)
dab.generate_bundle_yaml()        # databricks.yml
dab.generate_github_actions()     # .github/workflows/deploy.yml
dab.generate_job_resource(name, tasks)  # resources/<name>.yml
```

### AzureDevOpsPipeline
```python
from aws2lakehouse.cicd.azure_devops import AzureDevOpsPipeline
AzureDevOpsPipeline.generate(project_name)  # azure-pipelines.yml
```
