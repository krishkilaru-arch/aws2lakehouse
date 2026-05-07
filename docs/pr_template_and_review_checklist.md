# Pull Request Template
## .github/pull_request_template.md

### Description
<!-- What does this PR do? Link to JIRA/Linear ticket -->

### Pipeline Changes
- [ ] New pipeline added
- [ ] Existing pipeline modified
- [ ] Configuration change only
- [ ] Infrastructure/compute change

### Checklist
- [ ] Unit tests pass locally
- [ ] Integration tests pass on staging
- [ ] Data quality rules defined/updated
- [ ] Schema changes are backward-compatible
- [ ] MNPI classification reviewed (if applicable)
- [ ] Performance validated (no regression)
- [ ] Documentation updated
- [ ] Monitoring/alerting configured

### Data Impact
- **Tables affected:** 
- **Downstream consumers:** 
- **Expected volume change:** None / Increase / Decrease

### Rollback Plan
<!-- How to revert if something goes wrong -->

---

## Code Review Checklist (for reviewers)

### Data Engineering Standards
- [ ] Follows medallion architecture (Bronze/Silver/Gold)
- [ ] Uses Unity Catalog fully qualified names
- [ ] No hardcoded credentials or paths
- [ ] Secrets accessed via dbutils.secrets
- [ ] Idempotent design (safe to re-run)
- [ ] Proper error handling and logging

### Performance
- [ ] Appropriate partition strategy
- [ ] Z-ORDER on frequently filtered columns
- [ ] No unnecessary full table scans
- [ ] Broadcast joins for small tables
- [ ] Streaming uses checkpointing

### Governance
- [ ] Data classification tags applied
- [ ] MNPI columns masked (if applicable)
- [ ] Row-level filters (if applicable)
- [ ] Audit logging enabled
- [ ] Access grants follow least-privilege

### Testing
- [ ] Schema validation tests
- [ ] Data quality assertions
- [ ] Freshness/SLA checks
- [ ] Edge case handling (nulls, empty, duplicates)

### Operations
- [ ] Retry logic configured
- [ ] Timeout set appropriately
- [ ] Alerting on failure/SLA breach
- [ ] Runbook/documentation updated
