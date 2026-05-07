# Stakeholder Discovery Templates

## 1. Pipeline Owner Interview Questionnaire

Use this template for each pipeline domain (risk, lending, compliance, etc.)
during the Discovery phase. Complete one per business stakeholder.

---

### Section A: Business Context

| # | Question | Answer |
|---|----------|--------|
| A1 | What business process does this pipeline support? | |
| A2 | Who are the downstream consumers of this data? | |
| A3 | What decisions are made based on this data? | |
| A4 | What happens if this pipeline fails for 1 hour? 4 hours? 24 hours? | |
| A5 | Are there regulatory requirements (MiFID II, SOX, GDPR)? | |
| A6 | Is any of this data MNPI? Which fields? | |
| A7 | What is the data retention requirement? | |
| A8 | Who is the business owner vs. technical owner? | |

### Section B: Technical Requirements

| # | Question | Answer |
|---|----------|--------|
| B1 | Current data sources (list all) | |
| B2 | Current data targets (list all) | |
| B3 | Data volume (GB/day or rows/day) | |
| B4 | Processing frequency (batch/streaming/NRT) | |
| B5 | Current SLA (must complete by when?) | |
| B6 | Peak processing window | |
| B7 | Dependencies (upstream pipelines that must complete first) | |
| B8 | Downstream dependencies (what breaks if this is late?) | |
| B9 | Custom JARs or libraries used? | |
| B10 | Any UDFs or proprietary algorithms? | |

### Section C: Data Quality & Governance

| # | Question | Answer |
|---|----------|--------|
| C1 | Known data quality issues today? | |
| C2 | Critical business rules (e.g., "amount must never be negative") | |
| C3 | Who should have access to this data? | |
| C4 | Are there embargo periods? (data visible only after X hours) | |
| C5 | Column-level sensitivity? (which columns need masking?) | |
| C6 | Audit requirements? (who accessed what, when) | |

### Section D: Success Criteria

| # | Question | Answer |
|---|----------|--------|
| D1 | How will you validate the migrated pipeline produces correct results? | |
| D2 | Acceptable differences (if any) between old and new? | |
| D3 | Performance requirement (must be as fast or faster?) | |
| D4 | Monitoring/alerting requirements post-migration? | |
| D5 | Cutover preference (big-bang vs. parallel-run vs. gradual)? | |

---

## 2. SLA Capture Form

Fill one row per pipeline/dataset:

| Pipeline Name | Domain | Owner | Schedule | Must Complete By | Max Acceptable Delay | Impact of Breach | Notification Channel | Escalation Path |
|--------------|--------|-------|----------|-----------------|---------------------|------------------|---------------------|-----------------|
| daily_risk_calc | Risk | J. Smith | Daily 6am ET | 7:30am ET | 30 min | Trading desk blind | #risk-alerts | PagerDuty → VP Risk |
| loan_ingestion | Lending | A. Patel | Hourly | +15 min | 5 min | Stale applications | #lending-ops | Email → Dir. Lending |
| | | | | | | | | |

---

## 3. Success Metrics Tracker

Track these KPIs throughout the engagement:

### Migration Velocity
| Metric | Target | Week 1 | Week 4 | Week 8 | Week 12 | Week 16 | Week 20 |
|--------|--------|--------|--------|--------|---------|---------|---------|
| Pipelines migrated (cumulative) | 1000 | 3 | 15 | 100 | 350 | 700 | 1000 |
| Pipelines validated (cumulative) | 1000 | 3 | 15 | 100 | 350 | 700 | 1000 |
| Validation failures | 0 | | | | | | |
| Rollbacks required | 0 | | | | | | |

### Platform Health (post-migration)
| Metric | Target | Actual |
|--------|--------|--------|
| SLA compliance rate | >99% | |
| Data quality score (avg) | >95% | |
| Pipeline success rate (7d) | >99.5% | |
| Mean time to recovery (MTTR) | <30 min | |
| Cost vs. AWS baseline | -30% or better | |

### Business Outcomes
| Metric | Baseline (AWS) | Target (Databricks) | Actual |
|--------|---------------|--------------------|----- --|
| Time-to-insight (new report) | 3 weeks | 3 days | |
| Data freshness (avg staleness) | 4 hours | 15 min | |
| Operational incidents/month | 12 | <3 | |
| Engineer hours on maintenance/week | 120 | 40 | |

---

## 4. RACI Matrix Template

| Activity | Responsible | Accountable | Consulted | Informed |
|----------|------------|-------------|-----------|----------|
| Discovery sessions | Migration Team | Customer PM | Domain Owners | Exec Sponsor |
| Pipeline spec creation | Migration Team | Tech Lead | Pipeline Owners | |
| Code transformation | Migration Team | Tech Lead | Original Authors | |
| Validation testing | Migration Team + Customer | Customer PM | Business Owners | Exec Sponsor |
| Cutover decision | Customer PM | Exec Sponsor | Tech Lead | All Teams |
| Production monitoring | Customer Ops | Tech Lead | Migration Team | |
| Knowledge transfer | Migration Team | Tech Lead | Customer Engineers | |

---

## 5. Risk Register Template

| # | Risk | Likelihood | Impact | Mitigation | Owner | Status |
|---|------|-----------|--------|------------|-------|--------|
| R1 | Custom JARs incompatible with Databricks | Medium | High | JAR analyzer + early testing | Tech Lead | Open |
| R2 | MNPI data exposed during migration | Low | Critical | Parallel governance setup, never migrate without masks | Compliance | Open |
| R3 | SLA breach during cutover | Medium | High | Dual-write pattern, instant rollback | Ops Lead | Open |
| R4 | Team capacity insufficient for wave 3 | Medium | Medium | Buffer in schedule, automated factory reduces effort | PM | Open |
| R5 | Source data schema changes during migration | High | Medium | Schema evolution in Auto Loader, version pinning | Tech Lead | Open |
