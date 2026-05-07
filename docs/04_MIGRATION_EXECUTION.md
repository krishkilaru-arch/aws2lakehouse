# 4. Migration Execution & Engineering Excellence

## Purpose

Execute the migration of ~1000 EMR jobs to Databricks at scale, using the accelerator for automation and human judgment for edge cases.

---

## 4.1 Migration Workflow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MIGRATION EXECUTION FLOW                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SCAN           GENERATE         REVIEW          DEPLOY          VALIDATE   │
│  ─────          ────────         ──────          ──────          ────────   │
│                                                                             │
│  Scanners  →  SpecGenerator  →  Engineer  →  DAB Deploy  →  Validator      │
│  (auto)       + Factory         reviews       (CI/CD)        (auto)        │
│               (auto)            code                                        │
│                                                                             │
│  Time:        Time:            Time:          Time:          Time:          │
│  ~1 sec       ~1 sec           2-8 hrs        ~5 min         ~10 min       │
│  per 1000     per 1000         per pipeline   per deploy     per pipeline  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4.2 EMR → Databricks Compute Mapping

### Instance Type Mapping (141 AWS types → Databricks node types)

| AWS Family | AWS Type | Databricks Node | Use Case | Photon Speedup |
|-----------|----------|-----------------|----------|----------------|
| General | m5.xlarge | m5d.xlarge | Standard batch | 2x |
| General | m5.2xlarge | m5d.2xlarge | Medium batch | 2x |
| General | m6i.4xlarge | m6id.4xlarge | Large batch | 2.5x |
| Memory | r5.4xlarge | r5d.4xlarge | Joins/aggregations | 2x |
| Memory | r6i.8xlarge | r6id.8xlarge | Large shuffle | 2.5x |
| Compute | c5.4xlarge | c5d.4xlarge | CPU-intensive | 1.5x |
| Storage | i3.2xlarge | i3.2xlarge | I/O-intensive | 2x |
| GPU | p3.2xlarge | p3.2xlarge | ML training | 1x (no Photon) |

### Cluster Sizing Formula

```python
# EMR cluster → Databricks equivalent
def map_emr_to_databricks(emr_config):
    aws_workers = emr_config["worker_count"]
    aws_executor_memory = emr_config["executor_memory_gb"]
    
    # Photon gives 2-3x speedup → need fewer nodes
    photon_factor = 0.4  # 60% reduction
    
    dbx_workers = max(2, int(aws_workers * photon_factor))
    
    # Determine cluster type
    if emr_config.get("streaming"):
        cluster_type = "always_on"  # Streaming needs persistent cluster
    elif emr_config.get("schedule_frequency_min", 1440) < 60:
        cluster_type = "serverless"  # Frequent runs → serverless
    else:
        cluster_type = "job_cluster"  # Standard batch
    
    return {
        "node_type": map_instance_type(emr_config["instance_type"]),
        "min_workers": dbx_workers // 2,
        "max_workers": dbx_workers,
        "photon_enabled": True,
        "cluster_type": cluster_type,
        "estimated_monthly_cost": estimate_cost(dbx_workers, cluster_type),
    }
```

### Compute Decision Matrix

| Workload Pattern | Recommended Compute | Why |
|-----------------|--------------------|----|
| Daily batch (<1 hour) | Job cluster (auto-terminate) | Cost-efficient, isolated |
| Frequent batch (>4x/day) | Serverless | No startup time, pay per use |
| Streaming (continuous) | All-purpose cluster | Always available |
| ML Training | Job cluster + GPU | Isolated, right-sized |
| Interactive/dev | Shared cluster | Multi-user, cost-shared |
| SQL analytics | SQL Warehouse (serverless) | Optimized for queries |

---

## 4.3 Code Transformation Patterns

### Pattern 1: S3 Paths → Unity Catalog

```python
# BEFORE (AWS)
df.write.parquet("s3://acme-data-lake/gold/customers/")
df = spark.read.parquet("s3://acme-data-lake/silver/orders/")

# AFTER (Databricks)
df.write.mode("overwrite").saveAsTable("production.customer_gold.customers")
df = spark.table("production.commerce_silver.orders")
```

### Pattern 2: AWS Secrets Manager → Databricks Secrets

```python
# BEFORE (AWS)
import boto3
client = boto3.client("secretsmanager")
secret = json.loads(client.get_secret_value(SecretId="prod/postgres")["SecretString"])
url = secret["url"]

# AFTER (Databricks)
url = dbutils.secrets.get("postgres-prod", "url")
username = dbutils.secrets.get("postgres-prod", "username")
password = dbutils.secrets.get("postgres-prod", "password")
```

### Pattern 3: Hive Metastore → Unity Catalog

```python
# BEFORE
spark.sql("USE DATABASE risk_db")
spark.sql("INSERT INTO risk_db.trades SELECT ...")
df = spark.sql("SELECT * FROM risk_db.trades")

# AFTER
spark.sql("USE CATALOG production")
spark.sql("USE SCHEMA risk_silver")
spark.sql("INSERT INTO production.risk_silver.trades SELECT ...")
df = spark.table("production.risk_silver.trades")
```

### Pattern 4: Custom JARs → Native Alternatives

| Custom JAR Purpose | Databricks Native Alternative |
|-------------------|------------------------------|
| JSON parsing | `from_json()` + schema inference |
| Avro SerDe | Built-in Avro format support |
| Custom UDFs (Java) | Python UDFs or Pandas UDFs |
| Hive UDFs | Spark SQL functions (400+ built-in) |
| Custom partitioner | Liquid Clustering |
| S3 committer | Delta Lake (ACID commits) |
| Metrics/logging | System tables + Ganglia |
| Schema registry | Schema evolution + rescued data |

### Pattern 5: GlueContext → Native PySpark

```python
# BEFORE (Glue)
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
glueContext = GlueContext(sc)
dyf = glueContext.create_dynamic_frame.from_catalog(database="db", table_name="t")
df = dyf.toDF()
glueContext.write_dynamic_frame.from_options(frame=dyf, ...)

# AFTER (Databricks)
df = spark.table("production.domain_bronze.table_name")
# ... transforms ...
df.write.mode("overwrite").saveAsTable("production.domain_silver.table_name")
```

---

## 4.4 Configuration-Driven Framework

### The Key Insight

Instead of writing custom code for each pipeline, define pipeline behavior as configuration:

```yaml
# specs/trade_events_ingestion.yml
name: trade_events_ingestion
domain: risk
source:
  type: kafka
  topic: trade-events-v2
  secret_scope: kafka-risk-prod
  starting_offsets: latest
  max_offsets_per_trigger: 500000
target:
  catalog: production
  schema: risk_bronze
  table: trade_events
  mode: streaming
schedule:
  cron: continuous
  timeout_minutes: 0
governance:
  classification: mnpi
  owner: risk-engineering@acme.com
  mnpi_columns: [notional_amount, pnl, counterparty_name]
quality:
  - {type: not_null, column: trade_id}
  - {type: range, column: notional_amount, min: 0}
  - {type: freshness, column: event_timestamp, max_minutes: 5}
compute:
  cluster_type: streaming
  min_workers: 4
  max_workers: 16
  photon: true
```

### From Config → 6 Artifacts

```python
from aws2lakehouse.factory import PipelineFactory

factory = PipelineFactory(catalog="production")
artifacts = factory.generate(spec)

# artifacts.notebook_code      → src/pipelines/risk/bronze/trade_events.py
# artifacts.job_yaml           → resources/jobs/trade_events.yml
# artifacts.dq_sql             → quality/trade_events_expectations.sql
# artifacts.governance_sql     → governance/trade_events.sql
# artifacts.test_code          → tests/validation/test_trade_events.py
# artifacts.monitoring_sql     → monitoring/trade_events_monitoring.sql
```

---

## 4.5 Migration Prioritization

### Priority Matrix

| Priority | Criteria | Wave | Action |
|----------|----------|------|--------|
| P0 | Critical + Simple | Wave 0 (Pilot) | Migrate immediately |
| P1 | Critical + Complex | Wave 1 | Migrate with senior engineer |
| P2 | High + Simple/Medium | Wave 2 | Batch migrate with factory |
| P3 | Medium + Any | Wave 3-4 | Batch migrate (minimal review) |
| P4 | Low + Any | Final wave | Auto-migrate or deprecate |

### Batch Migration Velocity Targets

| Week | Target | Cumulative | Notes |
|------|--------|-----------|-------|
| 1-2 | Pilot (5) | 5 | Validate approach |
| 3-4 | 20/week | 45 | Wave 1 (high-value) |
| 5-8 | 40/week | 205 | Scale with patterns |
| 9-16 | 60/week | 685 | Full velocity |
| 17-20 | 80/week | 1005 | Remaining + complex |

---

## 4.6 Validation & Cutover

### Validation Checklist (Per Pipeline)

```python
# Auto-generated by MigrationValidator
validation_checks = [
    # 1. Row count comparison
    {"check": "row_count", "aws_count": 1_234_567, "dbx_count": 1_234_567, "match": True},
    
    # 2. Schema comparison
    {"check": "schema", "missing_cols": [], "type_mismatches": [], "match": True},
    
    # 3. Aggregate comparison (SUM, AVG, MIN, MAX per numeric column)
    {"check": "aggregates", "max_variance": 0.0001, "match": True},
    
    # 4. Sample hash comparison (random 1000 rows)
    {"check": "sample_hash", "rows_compared": 1000, "mismatches": 0, "match": True},
    
    # 5. Null count comparison
    {"check": "nulls", "max_diff_pct": 0.0, "match": True},
    
    # 6. SLA validation (timing)
    {"check": "sla", "aws_duration_min": 45, "dbx_duration_min": 12, "faster": True},
]
```

### Cutover Strategies

| Strategy | Risk | Downtime | Use When |
|----------|------|----------|----------|
| **Dual-Write** | Low | Zero | Critical pipelines, parallel validation |
| **Shadow Mode** | Low | Zero | Read from both, compare silently |
| **Blue-Green** | Medium | Minutes | Batch pipelines with defined cutover window |
| **Big-Bang** | High | Hours | Non-critical, well-tested |

---

## Deliverables Checklist

- [ ] All EMR clusters mapped to Databricks compute
- [ ] Code transformation rules defined and tested
- [ ] Configuration-driven framework producing correct artifacts
- [ ] Priority matrix defined and approved
- [ ] Validation framework passing for all pilot pipelines
- [ ] Cutover strategy selected per tier
- [ ] Rollback plan documented and tested
