# Wave Plan

> Migration roadmap for 5 pipelines across 1 domains

## Timeline Estimate

| Wave | Pipelines | Duration | Milestone |
|------|-----------|----------|-----------|
| Wave 0 (Pilot) | 3 | Week 1-2 | Validate approach |
| Wave 1 (Critical) | 0 | Week 3-4 | High-value pipelines live |
| Wave 2 (Core) | 0 | Week 5-6 | Majority migrated |
| Wave 3 (Remaining) | 2 | Week 7-8 | All pipelines migrated |

## Wave Details

### Wave 0 (Pilot) (3 pipelines)

| Pipeline | Domain | Source | Impact | Est. Effort |
|----------|--------|--------|--------|-------------|
| bronze_to_silver | finance | auto_loader | medium | 4h |
| silver_to_gold | finance | auto_loader | medium | 4h |
| bronze_ingestion | finance | auto_loader | medium | 4h |


### Wave 3 (2 pipelines)

| Pipeline | Domain | Source | Impact | Est. Effort |
|----------|--------|--------|--------|-------------|
| gold_aggregation | finance | auto_loader | medium | 4h |
| silver_transformation | finance | auto_loader | high | 4h |



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
