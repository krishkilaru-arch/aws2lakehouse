# Wave Plan

> Migration roadmap for 1 pipelines across 1 domains

## Timeline Estimate

| Wave | Pipelines | Duration | Milestone |
|------|-----------|----------|-----------|
| Wave 0 (Pilot) | 3 | Week 1-2 | Validate approach |
| Wave 1 (Critical) | 1 | Week 3-4 | High-value pipelines live |
| Wave 2 (Core) | 0 | Week 5-6 | Majority migrated |
| Wave 3 (Remaining) | 0 | Week 7-8 | All pipelines migrated |

## Wave Details

### Wave 1 (1 pipelines)

| Pipeline | Domain | Source | Impact | Est. Effort |
|----------|--------|--------|--------|-------------|
| ecommerce_daily_etl | analytics | delta_table | critical | 4h |



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
