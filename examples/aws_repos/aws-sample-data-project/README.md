# Acme Capital — AWS Data Platform (Legacy)

> This is a **sample representation** of a typical financial services AWS data platform
> with 12 pipelines across 4 domains, demonstrating common patterns that need migration.

## Current Architecture (AWS)

```
┌────────────────────────────────────────────────────────────────────────┐
│                     ACME CAPITAL — AWS DATA PLATFORM                    │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  SOURCES                    PROCESSING                 TARGETS         │
│  ───────                    ──────────                 ───────         │
│  Kafka (MSK)  ──────┐                                                 │
│    • trade-events    │      ┌─────────────────┐       S3 Data Lake    │
│    • market-data     ├─────▶│   EMR Cluster   │──────▶ s3://acme-     │
│    • order-flow      │      │ (Spark 2.4→3.1) │       │  data-lake/   │
│                      │      └─────────────────┘       │  /raw/        │
│  PostgreSQL (RDS)    │                                │  /curated/    │
│    • loan_apps       │      ┌─────────────────┐       │  /aggregated/ │
│    • customers       ├─────▶│   AWS Glue      │──────▶│               │
│    • payments        │      │  (PySpark ETL)  │       └───────────────│
│                      │      └─────────────────┘                       │
│  MongoDB Atlas       │                                 Redshift       │
│    • user_sessions   │      ┌─────────────────┐       (Reporting)    │
│    • click_events    ├─────▶│  Lambda + Step  │──────▶ Dashboards    │
│                      │      │  Functions      │                       │
│  S3 File Drops       │      └─────────────────┘                       │
│    • vendor CSVs     │                                                 │
│    • partner JSON    ├──── Airflow (MWAA) orchestrates everything     │
│    • compliance PDFs │                                                 │
└────────────────────────────────────────────────────────────────────────┘
```

## Pipeline Inventory (12 pipelines)

| # | Pipeline | Domain | Source | Type | Schedule | SLA | Complexity |
|---|----------|--------|--------|------|----------|-----|------------|
| 1 | trade_events_ingestion | Risk | Kafka | EMR Streaming | Continuous | 5 min | Complex |
| 2 | market_data_feed | Risk | Kafka | EMR Micro-batch | Every 1 min | 2 min | Medium |
| 3 | daily_risk_aggregation | Risk | S3 (from #1,#2) | EMR Batch | Daily 6am | 90 min | Complex |
| 4 | loan_application_etl | Lending | PostgreSQL | Glue | Hourly | 30 min | Medium |
| 5 | customer_360 | Customer | PostgreSQL + Mongo | Step Function | Daily 4am | 120 min | Complex |
| 6 | payment_processing | Lending | PostgreSQL | Glue | Every 15 min | 10 min | Simple |
| 7 | session_analytics | Analytics | MongoDB | Lambda | Hourly | 45 min | Simple |
| 8 | vendor_file_ingestion | Finance | S3 CSV drops | Glue | Triggered | 60 min | Simple |
| 9 | compliance_report | Compliance | S3 + RDS | Step Function | Daily 8am | 180 min | Complex |
| 10 | order_flow_capture | Risk | Kafka | EMR | Every 5 min | 8 min | Medium |
| 11 | customer_churn_model | Analytics | S3 (curated) | EMR (ML) | Weekly | 4 hr | Complex |
| 12 | partner_data_sync | Finance | S3 JSON + SFTP | Glue | Every 6 hr | 60 min | Medium |

## Dependencies

```
trade_events_ingestion (#1) ──┐
market_data_feed (#2)    ─────┼──▶ daily_risk_aggregation (#3)
order_flow_capture (#10) ─────┘

loan_application_etl (#4) ──┐
customer_360 (#5)      ─────┼──▶ compliance_report (#9)
payment_processing (#6) ────┘

session_analytics (#7) ─────────▶ customer_churn_model (#11)
customer_360 (#5) ──────────────▶ customer_churn_model (#11)
```

## MNPI / PII Data Classification

| Pipeline | MNPI Columns | PII Columns |
|----------|-------------|-------------|
| trade_events_ingestion | notional_amount, pnl, counterparty | |
| market_data_feed | bid_price, ask_price (pre-open) | |
| daily_risk_aggregation | var_amount, exposure | |
| loan_application_etl | | ssn, income, credit_score |
| customer_360 | | email, phone, address, dob |
| payment_processing | | account_number |

## Tech Stack
- **Compute:** EMR 6.9 (Spark 3.3.0), Glue 4.0 (Spark 3.3.0)
- **Orchestration:** Airflow 2.5 (MWAA), Step Functions, EventBridge
- **Storage:** S3, Redshift, DynamoDB
- **Streaming:** MSK (Kafka 3.3), Kinesis (legacy)
- **ML:** EMR + Spark MLlib, SageMaker (some models)
- **Secrets:** AWS Secrets Manager
- **Monitoring:** CloudWatch, custom Slack alerts
