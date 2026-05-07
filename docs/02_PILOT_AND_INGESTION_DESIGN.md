# 2. Pilot Implementation & Ingestion Design

## Purpose

The pilot phase proves the migration approach works before committing to 1000+ pipelines. Select 3-5 representative pipelines, migrate them end-to-end, and validate with stakeholders.

---

## 2.1 Pilot Pipeline Selection

### Selection Criteria

Choose ONE pipeline from each complexity tier that maximizes learning:

| Tier | Complexity | Ideal Characteristics | Example |
|------|-----------|----------------------|---------|
| Simple | 0-25 | Single source, batch, no dependencies | vendor_file_ingestion |
| Medium | 26-50 | JDBC source, some transforms, 1-2 deps | loan_application_etl |
| Complex | 51-75 | Streaming, multi-source, governance | trade_events_ingestion |

### Pilot Success Criteria

| Criteria | Simple | Medium | Complex |
|----------|--------|--------|---------|
| Data parity (row count match) | 100% | 100% | 99.9% |
| Schema compatibility | Exact match | Exact match | Exact match |
| SLA met | ✓ | ✓ | ✓ |
| Governance applied | Tags only | Tags + masks | Full MNPI |
| Monitoring active | Freshness | Freshness + volume | All 7 monitors |
| CI/CD deploying | Manual | Semi-auto | Full DAB |
| Stakeholder sign-off | Owner | Owner + consumer | Owner + compliance |

---

## 2.2 Ingestion Patterns

### Pattern 1: Auto Loader (File-Based Ingestion)

**Use when:** Files land in cloud storage (S3 → Volumes)
**Replaces:** Glue crawlers, S3 event triggers + Lambda, custom file watchers

```python
# Databricks Auto Loader with schema evolution
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")            # csv, json, parquet, avro
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")  # Captures schema mismatches
    .load(input_path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# Write as streaming append to Delta
(
    df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)                    # Process all available files, then stop
    .toTable(target_table)
)
```

**Key Features:**
- Automatic file detection (no Lambda/S3 event needed)
- Schema inference + evolution (handles new columns gracefully)
- Rescued data column (captures malformed records instead of failing)
- Exactly-once semantics via checkpointing
- `trigger(availableNow=True)` for batch-style scheduled runs

### Pattern 2: Delta Share with Change Data Feed (CDF)

**Use when:** Receiving data from external partners or cross-org sharing
**Replaces:** S3 cross-account copy + Glue crawlers, custom SFTP ingestion

```python
# Read shared table with CDF (only get changes since last read)
changes_df = (
    spark.readStream
    .format("deltaSharing")
    .option("startingVersion", "latest")
    .option("readChangeFeed", "true")
    .load(share_table_url)
)

# Process changes (inserts, updates, deletes)
changes_df = (
    changes_df
    .filter(F.col("_change_type").isin("insert", "update_postimage"))
    .drop("_change_type", "_commit_version", "_commit_timestamp")
)

# Merge into target (upsert pattern)
from delta.tables import DeltaTable
target = DeltaTable.forName(spark, target_table)
target.alias("t").merge(
    changes_df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Pattern 3: Kafka / Structured Streaming

**Use when:** Real-time event streams (replacing EMR Spark Streaming)
**Replaces:** EMR spark-submit with Kafka, Kinesis consumers, custom streaming apps

```python
# Read from Kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", 
            dbutils.secrets.get("kafka-prod", "bootstrap_servers"))
    .option("subscribe", "trade-events-v2")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "500000")
    .option("kafka.security.protocol", "SASL_SSL")
    .load()
)

# Parse JSON payload
parsed = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
    .withColumn("data", F.from_json("json_str", event_schema))
    .select("data.*", "kafka_ts")
    .withColumn("_ingested_at", F.current_timestamp())
)

# Write with trigger
(
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="30 seconds")  # or availableNow=True for batch
    .toTable(target_table)
)
```

### Pattern 4: JDBC (Database Extraction)

**Use when:** Extracting from PostgreSQL, MySQL, Oracle, SQL Server
**Replaces:** Glue JDBC connections, Sqoop, custom extraction scripts

```python
# Full load with parallel partitioned read
df = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get(scope, "url"))
    .option("user", dbutils.secrets.get(scope, "username"))
    .option("password", dbutils.secrets.get(scope, "password"))
    .option("dbtable", source_table)
    .option("partitionColumn", "id")
    .option("lowerBound", "0")
    .option("upperBound", str(max_id))
    .option("numPartitions", "20")
    .option("fetchsize", "10000")
    .load()
)

# Incremental with watermark
df_incremental = (
    spark.read.format("jdbc")
    .option("url", dbutils.secrets.get(scope, "url"))
    .option("dbtable", f"(SELECT * FROM {table} WHERE updated_at > '{last_watermark}') t")
    .load()
)
```

### Pattern 5: MongoDB

**Use when:** Extracting from document databases
**Replaces:** Custom MongoDB → S3 exporters, Glue MongoDB connections

```python
df = (
    spark.read
    .format("mongodb")
    .option("connection.uri", dbutils.secrets.get("mongodb", "uri"))
    .option("database", "analytics")
    .option("collection", "user_sessions")
    .option("pipeline", '[{"$match": {"created_at": {"$gte": ISODate("2024-01-01")}}}]')
    .load()
)
```

### Pattern 6: Snowflake

**Use when:** Migrating Snowflake-sourced pipelines
**Replaces:** Snowflake tasks, external stages, custom connectors

```python
df = (
    spark.read
    .format("snowflake")
    .options(**{
        "sfUrl": dbutils.secrets.get("snowflake", "url"),
        "sfUser": dbutils.secrets.get("snowflake", "user"),
        "sfPassword": dbutils.secrets.get("snowflake", "password"),
        "sfDatabase": "PROD",
        "sfSchema": "FINANCE",
        "sfWarehouse": "COMPUTE_WH",
        "dbtable": "TRANSACTIONS",
    })
    .load()
)
```

---

## 2.3 Medallion Architecture

### Layer Definitions

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MEDALLION ARCHITECTURE                              │
├─────────────┬───────────────────────────────────────────────────────────────┤
│             │                                                               │
│   BRONZE    │  Raw data, append-only, source-faithful                       │
│   (Raw)     │  • No business transforms                                    │
│             │  • Schema as-received (with rescue column)                    │
│             │  • Metadata: _ingested_at, _source_file, _batch_id            │
│             │  • Retention: 90 days (configurable)                          │
│             │  • Access: Data Engineering team only                         │
│             │                                                               │
├─────────────┼───────────────────────────────────────────────────────────────┤
│             │                                                               │
│   SILVER    │  Cleaned, conformed, deduplicated                             │
│   (Curated) │  • Type casting, null handling, standardization               │
│             │  • Deduplication (by business key + timestamp)                │
│             │  • Joins with reference data                                  │
│             │  • PII masking (column masks via Unity Catalog)               │
│             │  • Retention: 1 year                                          │
│             │  • Access: Domain teams (with governance)                     │
│             │                                                               │
├─────────────┼───────────────────────────────────────────────────────────────┤
│             │                                                               │
│    GOLD     │  Business-ready aggregates, KPIs, features                    │
│   (Serving) │  • Pre-aggregated for dashboards/reports                      │
│             │  • ML feature vectors                                         │
│             │  • Star schemas (facts + dimensions)                          │
│             │  • Retention: Permanent (versioned)                           │
│             │  • Access: Analysts, BI tools, ML models                     │
│             │                                                               │
└─────────────┴───────────────────────────────────────────────────────────────┘
```

### Naming Convention

```
{catalog}.{domain}_{layer}.{entity}

Examples:
  production.risk_bronze.trade_events
  production.risk_silver.trade_events_enriched
  production.risk_gold.daily_risk_metrics
  production.lending_bronze.loan_applications
  production.lending_silver.loan_applications_validated
  production.lending_gold.portfolio_summary
```

---

## 2.4 Orchestration Replacement

### Mapping Table

| AWS Service | Databricks Equivalent | Migration Tool |
|-------------|----------------------|----------------|
| Airflow DAG | Lakeflow Job (multi-task) | AirflowConverter |
| Step Functions | Lakeflow Job (IF/ELSE, ForEach) | StepFunctionConverter |
| Glue Triggers | Job schedule + file arrival | Auto Loader |
| Glue Workflows | Lakeflow Job | Manual design |
| EventBridge rules | File arrival trigger | Auto Loader |
| Lambda (light ETL) | Notebook task | CodeTransformer |
| cron + EC2 | Scheduled Job | Direct mapping |

### Multi-Task Workflow Example

```yaml
resources:
  jobs:
    daily_risk_pipeline:
      name: "[prod] Daily Risk Pipeline"
      schedule:
        quartz_cron_expression: "0 0 6 * * ? *"
        timezone_id: "America/New_York"
      tasks:
        - task_key: bronze_trade_events
          notebook_task:
            notebook_path: src/pipelines/risk/bronze/trade_events.py
          
        - task_key: bronze_market_data
          notebook_task:
            notebook_path: src/pipelines/risk/bronze/market_data.py
          
        - task_key: silver_enrichment
          depends_on: [{task_key: bronze_trade_events}, {task_key: bronze_market_data}]
          notebook_task:
            notebook_path: src/pipelines/risk/silver/trade_enrichment.py
          
        - task_key: gold_risk_metrics
          depends_on: [{task_key: silver_enrichment}]
          notebook_task:
            notebook_path: src/pipelines/risk/gold/daily_risk_metrics.py
          condition_task:
            # Only run if silver quality passed
            left: "{{tasks.silver_enrichment.values.dq_passed}}"
            op: "EQUAL_TO"
            right: "true"
```

---

## Deliverables Checklist

- [ ] Pilot pipelines selected (simple + medium + complex)
- [ ] Ingestion patterns documented and tested
- [ ] Medallion architecture defined (naming, access, retention)
- [ ] Orchestration mapping completed
- [ ] Pilot validation passed (data parity confirmed)
- [ ] Stakeholder sign-off on pilot results
- [ ] Lessons learned documented
- [ ] Patterns ready for Wave 1 scale-out
