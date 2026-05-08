# Migration Validation Checklist

> Complete ALL checks before cutting over each pipeline to production

## Pre-Cutover (Per Pipeline)

- [ ] **daily_risk_aggregation**: Row count ✓ | Schema ✓ | Aggregates ✓ | SLA ✓
- [ ] **loan_application_etl**: Row count ✓ | Schema ✓ | Aggregates ✓ | SLA ✓
- [ ] **customer_360**: Row count ✓ | Schema ✓ | Aggregates ✓ | SLA ✓
- [ ] **trade_events_streaming**: Row count ✓ | Schema ✓ | Aggregates ✓ | SLA ✓
- [ ] **customer_churn_model**: Row count ✓ | Schema ✓ | Aggregates ✓ | SLA ✓
- [ ] **market_data_feed**: Row count ✓ | Schema ✓ | Aggregates ✓ | SLA ✓
- [ ] **vendor_file_ingestion**: Row count ✓ | Schema ✓ | Aggregates ✓ | SLA ✓
- [ ] **payment_processing**: Row count ✓ | Schema ✓ | Aggregates ✓ | SLA ✓

## Validation Steps

### 1. Data Parity
```bash
# Run auto-generated validation tests
databricks bundle run validation-tests --target staging
```

Each test in `tests/validation/test_{pipeline}.py` checks:
- Row count comparison (AWS vs Databricks)
- Schema match (column names + types)
- Aggregate comparison (SUM, AVG, MIN, MAX)
- Null count comparison
- Sample record hash

### 2. SLA Verification
- [ ] Run pipeline 3 consecutive days
- [ ] All runs complete within SLA (5-240 min depending on pipeline)
- [ ] No SLA breaches

### 3. Governance Verification
- [ ] All tables tagged (classification, domain, owner)
- [ ] Column masks applied (test with non-privileged user)
- [ ] Row filters working (test embargo periods if applicable)
- [ ] Audit log capturing access events

### 4. Monitoring Verification
- [ ] Freshness alerts firing correctly
- [ ] Volume anomaly detection working
- [ ] DQ scores being recorded
- [ ] Slack/email notifications delivering

## Post-Cutover (7-Day Hypercare)

- [ ] Day 1-3: Monitor all pipeline runs, compare with AWS
- [ ] Day 4-5: Disable AWS pipeline (keep data for comparison)
- [ ] Day 6-7: Confirm no consumer complaints
- [ ] Day 7: Final sign-off from pipeline owner
- [ ] Decommission AWS resources

## Rollback Plan

If issues found during hypercare:
1. Re-enable AWS pipeline
2. Notify consumers
3. Investigate root cause
4. Fix and re-validate
5. Retry cutover
