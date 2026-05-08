# Cutover & Hypercare Playbook

## Zero-Downtime Migration Patterns

This playbook covers the three cutover strategies, when to use each, and
exact steps for execution, monitoring, and rollback.

---

## Strategy Comparison

| Strategy | Risk | Complexity | Data Lag | Best For |
|----------|------|-----------|----------|----------|
| Dual-Write | Low | High | None | Critical/MNPI pipelines |
| Shadow Mode | Low | Medium | Minutes | High-impact pipelines |
| Big-Bang | Medium | Low | None | Simple/batch pipelines |

---

## Strategy 1: Dual-Write (Recommended for Critical Pipelines)

**Pattern:** Both old and new pipelines write to their own targets simultaneously.
A comparison job validates they produce identical results.

### Architecture
```
[Source] ──┬──> [Legacy Pipeline] ──> [legacy_db.table]
           │                              │
           │                              ▼
           │                    [Comparison Job] ──> Alert if drift
           │                              ▲
           └──> [New Pipeline] ─────> [new_db.table]
```

### Implementation Steps

#### Phase A: Setup Dual-Write (Week N-2)
1. Deploy new pipeline to production (writing to separate target)
2. Configure same schedule as legacy
3. Enable monitoring on both
4. Deploy comparison job (runs after both complete)

```yaml
# Dual-write comparison job (DAB resource)
resources:
  jobs:
    dual_write_validation:
      name: "[cutover] {pipeline}_dual_write_check"
      schedule:
        quartz_cron_expression: "0 30 7 ? * * *"  # 30 min after both should complete
      tasks:
        - task_key: compare_outputs
          notebook_task:
            notebook_path: /cutover/compare_tables
            base_parameters:
              LEGACY_TABLE: "legacy_db.target_table"
              NEW_TABLE: "new_db.target_table"
              PRIMARY_KEY: "id"
              TOLERANCE_PCT: "0.001"
          max_retries: 0
```

#### Phase B: Validate (Week N-2 to N)
- Run dual-write for minimum 2 weeks (covers business cycles)
- Monitor comparison job daily
- Track: row count drift, aggregate drift, schema changes
- **Exit criteria:** 14 consecutive days with 0 drift

#### Phase C: Cutover (Day N)
1. Disable legacy pipeline schedule
2. Point downstream consumers to new table
3. Keep legacy pipeline code (do NOT delete for 30 days)

#### Phase D: Rollback (if needed)
1. Re-enable legacy pipeline schedule
2. Revert downstream pointers
3. Timeline: <5 minutes to execute

---

## Strategy 2: Shadow Mode (Recommended for High-Impact)

**Pattern:** New pipeline runs in "shadow" — produces output but doesn't serve consumers.
Validation runs asynchronously. Cutover is just flipping a pointer.

### Implementation Steps

1. Deploy new pipeline writing to `_shadow` schema
2. Run validation suite nightly against shadow output
3. After 7+ days passing, cutover = rename view/table pointer

```sql
-- Pre-cutover: consumers read from view
CREATE OR REPLACE VIEW production.risk_gold.daily_pnl AS
SELECT * FROM production.risk_gold._legacy_daily_pnl;

-- Cutover: flip the view (instant, atomic)
CREATE OR REPLACE VIEW production.risk_gold.daily_pnl AS
SELECT * FROM production.risk_gold._new_daily_pnl;

-- Rollback: flip back (instant)
CREATE OR REPLACE VIEW production.risk_gold.daily_pnl AS
SELECT * FROM production.risk_gold._legacy_daily_pnl;
```

**Advantage:** Zero-downtime, instant rollback via VIEW swap.

---

## Strategy 3: Big-Bang (For Simple/Batch Pipelines)

**Pattern:** Validate in staging, then switch schedule in one maintenance window.

### When to use:
- Batch pipelines with low business impact
- No real-time consumers
- Simple logic (lift-and-shift)

### Steps:
1. Validate in staging for 3+ successful runs
2. Pick maintenance window (e.g., Saturday night)
3. Disable old, enable new, verify next morning
4. Rollback: re-enable old within the day

---

## Hypercare Phase (Post-Cutover: 7-14 days)

### Day 1 Checklist
- [ ] First production run completed successfully
- [ ] SLA met (pipeline finished before deadline)
- [ ] Row count within tolerance of historical average
- [ ] No DQ alerts fired
- [ ] Downstream reports confirmed correct (spot-check 5 records)
- [ ] Monitoring dashboard green

### Day 2-7 Routine
| Time | Action | Owner |
|------|--------|-------|
| 8:00 AM | Check monitoring dashboard | Ops |
| 8:15 AM | Review any overnight alerts | Migration Lead |
| 9:00 AM | Verify downstream reports | Business Owner |
| 5:00 PM | End-of-day status update | Migration Lead |

### Day 7 Exit Criteria
- [ ] 7 consecutive successful runs
- [ ] No manual interventions required
- [ ] DQ scores stable (no regression from baseline)
- [ ] Performance within 10% of benchmark
- [ ] Business owner sign-off
- [ ] Legacy pipeline can be archived (disable, don't delete)

### Day 14 Final Handoff
- [ ] Runbook complete and reviewed by ops team
- [ ] Alert routing configured (no longer goes to migration team)
- [ ] Legacy pipeline code archived to cold storage
- [ ] Migration tracking table updated: status = "complete"
- [ ] Knowledge transfer session completed

---

## Backfill Strategies

### Full Historical Backfill
When: New pipeline needs all historical data (not just going-forward)

```python
# Generate date ranges for parallel backfill
from datetime import date, timedelta

start = date(2020, 1, 1)
end = date(2026, 5, 1)
chunk_days = 7

ranges = []
current = start
while current < end:
    chunk_end = min(current + timedelta(days=chunk_days), end)
    ranges.append({"start": str(current), "end": str(chunk_end)})
    current = chunk_end

# Execute via for_each_task in workflow (5 parallel)
# See: WorkflowTemplates.backfill_workflow()
```

### Incremental Backfill (Preferred)
When: Source supports watermark/CDC

1. Start new pipeline with `starting_offsets: earliest` or `readStream.option("startingVersion", 0)`
2. Let it catch up at its own pace
3. Once caught up, switch to normal schedule

### Partition-by-Partition Backfill
When: Target is partitioned by date

```python
# Process one partition at a time (safe for reruns)
for partition_date in date_range:
    spark.sql(f"""
        INSERT OVERWRITE production.risk_bronze.trades
        PARTITION (trade_date = '{partition_date}')
        SELECT * FROM staging.risk_bronze.trades
        WHERE trade_date = '{partition_date}'
    """)
```

---

## Rollback Decision Matrix

| Scenario | Action | Timeline |
|----------|--------|----------|
| Data mismatch <0.1% | Investigate, don't rollback | Hours |
| Data mismatch >1% | Rollback immediately | Minutes |
| Pipeline failure (first run) | Debug, retry once | 30 min |
| Pipeline failure (2nd time) | Rollback | Minutes |
| SLA breach (first occurrence) | Investigate cause | Hours |
| SLA breach (2nd consecutive) | Rollback | Minutes |
| Downstream report wrong | Rollback immediately | Minutes |
| Performance 2x slower | Optimize, don't rollback (unless SLA breach) | Days |

---

## Communication Templates

### Pre-Cutover Notification (T-2 days)
```
Subject: [Data Platform] Scheduled Migration: {pipeline_name} - {cutover_date}

Team,

We are migrating {pipeline_name} from AWS EMR to Databricks on {cutover_date}.

What's changing:
- Source table remains the same
- Processing moves to Databricks (faster, more reliable)
- Target table: {new_table_name}
- No action needed from consumers (view pointer will be updated)

Validation results: 14 days of dual-write with 0 drift
Rollback plan: Instant view swap if any issues

Contact: {migration_lead} | Slack: #data-migration-{domain}
```

### Post-Cutover Confirmation (T+1)
```
Subject: [Data Platform] Migration Complete: {pipeline_name} ✓

Team,

{pipeline_name} successfully migrated. First production run:
- Completed at: {completion_time}
- SLA status: MET ({duration} vs {sla} target)
- Row count: {row_count} (within tolerance)
- DQ score: {dq_score}%

We are in hypercare mode for 7 days. Any issues → #data-migration-{domain}

Legacy pipeline archived (not deleted, available for rollback if needed).
```

### Rollback Notification
```
Subject: [Data Platform] ROLLBACK: {pipeline_name} - Reverted to Legacy

Team,

We have rolled back {pipeline_name} to the legacy AWS pipeline.

Reason: {reason}
Impact: {impact_description}
Resolution ETA: {eta}

Data consumers: You are now receiving data from the legacy system (no action needed).
Next steps: Investigation in progress, will schedule new cutover window.

Status updates: #data-migration-{domain}
```
