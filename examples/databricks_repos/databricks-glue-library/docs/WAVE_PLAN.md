# Wave Plan

> Migration roadmap for 9 pipelines across 1 domains

## Timeline Estimate

| Wave | Pipelines | Duration | Milestone |
|------|-----------|----------|-----------|
| Wave 0 (Pilot) | 3 | Week 1-2 | Validate approach |
| Wave 1 (Critical) | 0 | Week 3-4 | High-value pipelines live |
| Wave 2 (Core) | 4 | Week 5-6 | Majority migrated |
| Wave 3 (Remaining) | 2 | Week 7-8 | All pipelines migrated |

## Wave Details

### Wave 0 (Pilot) (3 pipelines)

| Pipeline | Domain | Source | Impact | Est. Effort |
|----------|--------|--------|--------|-------------|
| feature_store_builder | finance | auto_loader | medium | 4h |
| order_events_processing | finance | auto_loader | medium | 4h |
| vendor_data_ingestion | finance | auto_loader | medium | 4h |


### Wave 2 (4 pipelines)

| Pipeline | Domain | Source | Impact | Est. Effort |
|----------|--------|--------|--------|-------------|
| position_snapshot_job | finance | jdbc | medium | 1-2d |
| nav_calculation_job | finance | jdbc | medium | 1-2d |
| customer_dimension | finance | jdbc | high | 1-2d |
| financial_reconciliation | finance | jdbc | high | 1-2d |


### Wave 3 (2 pipelines)

| Pipeline | Domain | Source | Impact | Est. Effort |
|----------|--------|--------|--------|-------------|
| market_data_ingest_job | finance | auto_loader | medium | 4h |
| risk_aggregation_job | finance | auto_loader | medium | 4h |



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
