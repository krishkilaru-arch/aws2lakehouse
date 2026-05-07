# Pipeline YAML Reference

Complete specification for pipeline YAML files used by the PipelineFactory.

## Minimal Spec (Required Fields Only)

```yaml
name: my_pipeline
source:
  type: auto_loader
  config:
    path: /Volumes/prod/raw/data/
target:
  table: my_table
```

## Full Spec (All Available Options)

```yaml
# ═══════════════════════════════════════════════════════════════
# PIPELINE IDENTITY
# ═══════════════════════════════════════════════════════════════
name: trade_events                    # Required. Snake_case, unique within domain
domain: risk                          # Business domain (risk, lending, customer, etc.)
owner: risk-engineering@company.com   # Team email or Slack handle
description: "Real-time trade event processing with MNPI controls"
layer: bronze                         # bronze | silver | gold
depends_on:                           # Other pipeline names (for DAG ordering)
  - market_data_feed
  - reference_data_load

# ═══════════════════════════════════════════════════════════════
# SOURCE CONFIGURATION
# ═══════════════════════════════════════════════════════════════
source:
  type: kafka                         # See "Source Types" below
  config:
    # Type-specific configuration (see examples per type)
    topic: trade-events-v2
    secret_scope: kafka-risk-prod
    starting_offsets: latest
    max_offsets: "500000"

# ═══════════════════════════════════════════════════════════════
# TARGET CONFIGURATION
# ═══════════════════════════════════════════════════════════════
target:
  catalog: production                 # Unity Catalog catalog name
  schema: risk_bronze                 # Schema within catalog
  table: trade_events                 # Table name
  format: delta                       # Always delta (default)
  mode: streaming                     # streaming | batch | merge
  partition_by:                       # Low-cardinality columns (1-2 max)
    - trade_date
  z_order_by:                         # High-cardinality filter columns (1-4)
    - trade_id
    - counterparty_id
  merge_keys:                         # Required for mode=merge (SCD keys)
    - trade_id
    - trade_version
  scd_type: 1                         # 1 (overwrite) or 2 (history)
  soft_delete: false                  # For merge: delete unmatched source rows

# ═══════════════════════════════════════════════════════════════
# DATA QUALITY RULES
# ═══════════════════════════════════════════════════════════════
quality:
  - name: valid_trade_id              # Rule name (alphanumeric + underscore)
    condition: "trade_id IS NOT NULL AND length(trade_id) > 0"
    action: fail                      # warn | drop | quarantine | fail
    threshold: 1.0                    # % of rows that must pass (0.0-1.0)
  
  - name: valid_notional
    condition: "notional_amount > 0"
    action: drop                      # Remove bad rows silently
  
  - name: valid_currency
    condition: "currency IN ('USD','EUR','GBP','JPY')"
    action: warn                      # Log but keep
    threshold: 0.98                   # Allow 2% tolerance

# ═══════════════════════════════════════════════════════════════
# GOVERNANCE & COMPLIANCE
# ═══════════════════════════════════════════════════════════════
governance:
  classification: mnpi                # public | internal | confidential | mnpi
  embargo_hours: 24                   # Hours before data becomes public
  mnpi_columns:                       # Columns tagged as MNPI
    - notional_amount
    - pnl
    - counterparty_name
  column_masks:                       # Column → mask expression (when unauthorized)
    notional_amount: "0.00"
    counterparty_name: "'[RESTRICTED]'"
    ssn: "CONCAT('***-**-', RIGHT(ssn, 4))"
  row_filter: "trade_date < current_date() - INTERVAL 1 DAY"
  owner_group: risk_trading_desk      # Group with full access

# ═══════════════════════════════════════════════════════════════
# TRANSFORMATION (Optional — for Silver/Gold layers)
# ═══════════════════════════════════════════════════════════════
transform_sql: |
  SELECT
    trade_id,
    counterparty_id,
    CAST(notional_amount AS DECIMAL(18,2)) as notional,
    UPPER(currency) as currency,
    to_date(trade_timestamp) as trade_date
  FROM source_data
  WHERE trade_status != 'CANCELLED'

# OR reference an existing notebook:
# transform_notebook: /Workspace/transforms/risk/enrich_trades

# ═══════════════════════════════════════════════════════════════
# COMPUTE PREFERENCES
# ═══════════════════════════════════════════════════════════════
compute:
  type: serverless                    # serverless | job_cluster | shared
  photon: true                        # Enable Photon acceleration
  node_type: r5.2xlarge               # Only for job_cluster
  min_workers: 2                      # Only for job_cluster
  max_workers: 8                      # Only for job_cluster

# ═══════════════════════════════════════════════════════════════
# SCHEDULE & RETRY
# ═══════════════════════════════════════════════════════════════
schedule:
  cron: "*/5 * * * *"                 # Standard 5-field cron
  timezone: America/New_York          # IANA timezone
  sla_minutes: 10                     # Alert if not complete within

retry:
  max_attempts: 5                     # Total attempts (including first)
  backoff_seconds: 30                 # Wait between retries

# ═══════════════════════════════════════════════════════════════
# ALERTING
# ═══════════════════════════════════════════════════════════════
alerting:
  on_failure:                         # Notify on pipeline failure
    - slack:#risk-data-alerts
    - pagerduty:risk-oncall
  on_sla_breach:                      # Notify on SLA miss
    - pagerduty:risk-oncall
    - email:risk-leads@company.com
  on_success:                         # Notify on success (optional, usually empty)
    - slack:#risk-data-success

# ═══════════════════════════════════════════════════════════════
# METADATA TAGS
# ═══════════════════════════════════════════════════════════════
tags:
  cost_center: CC-RISK-001
  criticality: critical               # low | medium | high | critical
  regulatory: mifid2                  # Regulatory framework
  team: risk-quant
```

## Source Types

### `auto_loader` — File Ingestion (most common for Bronze)

```yaml
source:
  type: auto_loader
  config:
    path: /Volumes/production/raw/transactions/    # Source file path
    format: json                       # csv | json | parquet | avro | orc | text
    # CSV-specific options:
    header: "true"
    delimiter: ","
    # Notification mode (recommended for production):
    use_notifications: "true"
```

### `kafka` — Streaming Events

```yaml
source:
  type: kafka
  config:
    secret_scope: kafka-prod           # Databricks secret scope
    topic: trade-events                # Kafka topic name
    starting_offsets: latest            # earliest | latest | specific JSON
    max_offsets: "100000"              # Max records per micro-batch
    security_protocol: SASL_SSL        # PLAINTEXT | SASL_SSL | SSL
```

### `delta_share` — Cross-Organization Sharing

```yaml
source:
  type: delta_share
  config:
    share_name: vendor_data            # Delta Share name
    schema: transactions               # Schema in share
    table: daily_trades                # Table in share
    starting_version: "0"              # Start from version 0 for CDF
```

### `jdbc` — Database Extraction (PostgreSQL, MySQL, Oracle)

```yaml
source:
  type: jdbc
  config:
    secret_scope: postgres-prod        # Contains url, username, password
    source_table: public.orders        # Source table
    partition_column: id               # Column for parallel reads
    num_partitions: "20"               # Parallel connections
    fetchsize: "10000"                 # Rows per fetch
```

### `mongodb` — Document Store

```yaml
source:
  type: mongodb
  config:
    secret_scope: mongo-prod           # Contains connection URI
    database: production               # MongoDB database
    collection: customers              # Collection name
```

### `snowflake` — Snowflake Extraction

```yaml
source:
  type: snowflake
  config:
    secret_scope: snowflake-prod       # Contains account, user, password
    database: ANALYTICS
    schema: PUBLIC
    warehouse: COMPUTE_WH
    source_table: DAILY_METRICS
```

### `delta_table` — Internal Table (for Silver/Gold)

```yaml
source:
  type: delta_table
  config:
    source_table: production.risk_bronze.trade_events
```

## Quality Rule Actions

| Action | Behavior | Use When |
|--------|----------|----------|
| `warn` | Log violation, keep row | Non-critical data quality |
| `drop` | Silently remove bad rows | Known bad data patterns |
| `quarantine` | Move to `_quarantine` table | Need to investigate later |
| `fail` | Abort pipeline | Critical data integrity |

## Generated Artifacts

When you run `factory.generate(spec)`, you get:

| Artifact | Contents | Deploy To |
|----------|----------|-----------|
| `notebook_code` | Complete Python notebook with source/transform/quality/sink | Workspace |
| `job_yaml` | DAB-compatible job resource with schedule/retry/tags | `resources/` |
| `dq_sql` | SDP CONSTRAINT/EXPECT statements | SDP pipeline |
| `governance_sql` | ALTER TABLE tags, CREATE FUNCTION masks, row filters | SQL warehouse |
| `test_code` | pytest functions for table/schema/quality/freshness | CI/CD pipeline |
| `monitoring_sql` | Freshness + volume anomaly queries | Observability DB |

## Tips

1. **Start simple** — Use `auto_loader` + `streaming` for Bronze. Add complexity in Silver/Gold.
2. **One YAML per table** — Each spec produces exactly one target table.
3. **Use `depends_on`** — The factory uses this to set job task dependencies.
4. **Secrets in scopes** — Never put credentials in YAML. Use `secret_scope` references.
5. **Governance early** — Set `classification` from day 1. Adding masks later is harder.
6. **SLA = alerting** — Always set `sla_minutes` if you set `alerting.on_sla_breach`.
