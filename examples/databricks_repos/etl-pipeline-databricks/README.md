# ShopMax Analytics Platform (Databricks)

Migrated from: EMR + Airflow + Kafka + Redshift → Databricks Lakehouse

## Architecture Mapping

| AWS (Before) | Databricks (After) |
|---|---|
| Airflow DAG (5 tasks) | Multi-task Job YAML (5 tasks) |
| EMR transient cluster (8x r5.4xl) | Job cluster with Photon |
| EMR streaming (12x m5.2xl 24/7) | Streaming job every 5 min (90% cheaper) |
| Redshift (4x ra3.4xl) | Delta Lake Gold tables (eliminated!) |
| Custom Python extractors | Native JDBC + Auto Loader |
| S3 Parquet intermediate | Delta Lake (ACID, Z-ORDER, time travel) |

## Cost Savings
- EMR eliminated: -$45K/mo
- Redshift eliminated: -$12K/mo  
- Databricks cost: +$22K/mo
- **Net savings: $35K/month (43%)**
