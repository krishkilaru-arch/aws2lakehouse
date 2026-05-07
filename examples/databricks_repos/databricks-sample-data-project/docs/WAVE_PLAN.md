# Wave Plan

> Migration roadmap for 8 pipelines across 5 domains

## Timeline Estimate

| Wave | Pipelines | Duration | Milestone |
|------|-----------|----------|-----------|
| Wave 0 (Pilot) | 3 | Week 1-2 | Validate approach |
| Wave 1 (Critical) | 4 | Week 3-4 | High-value pipelines live |
| Wave 2 (Core) | 1 | Week 5-6 | Majority migrated |
| Wave 3 (Remaining) | 0 | Week 7-8 | All pipelines migrated |

## Wave Details

### Wave 0 (Pilot) (3 pipelines)

| Pipeline | Domain | Source | Impact | Est. Effort |
|----------|--------|--------|--------|-------------|
| customer_churn_model | analytics | delta_table | medium | 4h |
| vendor_file_ingestion | finance | auto_loader | medium | 4h |
| loan_application_etl | lending | jdbc | high | 1-2d |


### Wave 1 (4 pipelines)

| Pipeline | Domain | Source | Impact | Est. Effort |
|----------|--------|--------|--------|-------------|
| daily_risk_aggregation | risk | delta_table | critical | 4h |
| customer_360 | customer | kafka | critical | 1-2d |
| trade_events_streaming | risk | kafka | critical | 1-2d |
| market_data_feed | risk | kafka | critical | 1-2d |


### Wave 2 (1 pipelines)

| Pipeline | Domain | Source | Impact | Est. Effort |
|----------|--------|--------|--------|-------------|
| payment_processing | lending | jdbc | high | 1-2d |



## Dependencies

Pipelines are ordered to respect data dependencies:
- Bronze pipelines migrate first (no upstream deps)
- Silver pipelines follow (depend on bronze)
- Gold pipelines last (depend on silver)

## Success Criteria Per Wave

| Wave | Criteria |
|------|----------|
| Pilot | Data parity confirmed, SLA met, governance applied |
| Wave 1 | All critical pipelines live, no SLA regression |
| Wave 2 | All core pipelines live, monitoring active |
| Wave 3 | All pipelines migrated, AWS decommissioned |
