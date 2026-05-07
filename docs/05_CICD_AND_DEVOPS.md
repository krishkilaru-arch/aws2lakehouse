# 5. CI/CD, DevOps & Platform Standards

## Purpose

Establish production-grade deployment pipelines using Databricks Asset Bundles (DABs), enabling federated development with strong quality gates.

---

## 5.1 Databricks Asset Bundles (DABs)

### What is a DAB?

A DAB is a **declarative, version-controlled definition** of your entire Databricks project:
- Notebooks, Python files, SQL scripts
- Job definitions (schedule, compute, tasks, retries)
- Cluster configurations
- Permissions and access control
- All deployable with one command: `databricks bundle deploy`

### Bundle Structure

```
my-data-project/
├── databricks.yml              # Bundle entry point
├── resources/
│   ├── jobs/                   # Job YAML definitions
│   │   ├── trade_events.yml
│   │   ├── daily_risk.yml
│   │   └── customer_360.yml
│   └── clusters/              # Shared cluster configs
│       └── streaming.yml
├── src/
│   ├── pipelines/             # Notebook code (per domain/layer)
│   │   ├── risk/bronze/
│   │   ├── risk/silver/
│   │   └── risk/gold/
│   └── common/                # Shared utilities
│       ├── dq_checks.py
│       └── notifications.py
├── governance/                 # SQL for masks, filters, grants
├── quality/                    # DQ expectation definitions
├── monitoring/                 # Monitoring SQL views
├── tests/                      # Validation + unit tests
│   ├── unit/
│   └── validation/
├── .github/workflows/          # CI/CD pipeline
│   └── deploy.yml
└── .gitignore
```

### databricks.yml Example

```yaml
bundle:
  name: acme-risk-platform

workspace:
  host: https://your-workspace.cloud.databricks.com

variables:
  catalog:
    description: "Target Unity Catalog"
    default: development
  environment:
    description: "Deployment environment"

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: development
      environment: dev

  staging:
    mode: development
    variables:
      catalog: staging
      environment: staging
    run_as:
      service_principal_name: acme-staging-sp

  production:
    mode: production
    variables:
      catalog: production
      environment: prod
    run_as:
      service_principal_name: acme-production-sp
    permissions:
      - group_name: data-engineering
        level: CAN_MANAGE
      - group_name: data-analysts
        level: CAN_VIEW

include:
  - resources/jobs/*.yml
  - resources/clusters/*.yml
```

---

## 5.2 CI/CD Pipeline

### GitHub Actions (Primary)

```yaml
# .github/workflows/deploy.yml
name: Databricks CI/CD

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      
      - name: Validate bundle
        run: databricks bundle validate --target staging
      
      - name: Run unit tests
        run: |
          pip install pytest pyspark
          pytest tests/unit/ -v

  deploy-staging:
    needs: validate
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target staging
      
      - name: Run integration tests
        run: databricks bundle run integration-tests --target staging

  deploy-production:
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production  # Requires manual approval
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target production
      
      - name: Smoke test
        run: databricks bundle run smoke-tests --target production
```

### Azure DevOps (Alternative)

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include: [main, develop]

stages:
  - stage: Validate
    jobs:
      - job: ValidateBundle
        pool: {vmImage: ubuntu-latest}
        steps:
          - script: |
              curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
              databricks bundle validate --target staging

  - stage: DeployStaging
    dependsOn: Validate
    condition: eq(variables['Build.SourceBranch'], 'refs/heads/develop')
    jobs:
      - deployment: StagingDeploy
        environment: staging
        strategy:
          runOnce:
            deploy:
              steps:
                - script: databricks bundle deploy --target staging

  - stage: DeployProduction
    dependsOn: Validate
    condition: eq(variables['Build.SourceBranch'], 'refs/heads/main')
    jobs:
      - deployment: ProductionDeploy
        environment: production  # Manual approval gate
        strategy:
          runOnce:
            deploy:
              steps:
                - script: databricks bundle deploy --target production
```

---

## 5.3 Branching Strategy

### Git Flow (Recommended for Regulated Environments)

```
main (production)
  │
  ├── develop (staging/integration)
  │     │
  │     ├── feature/risk-trade-events-migration
  │     ├── feature/lending-loan-etl-migration
  │     ├── fix/market-data-schema-issue
  │     └── ...
  │
  └── hotfix/critical-sla-fix (emergency only)
```

### Branch Rules

| Branch | Who can push | Requires PR | Requires approval | Auto-deploy to |
|--------|-------------|-------------|-------------------|----------------|
| main | Nobody (PR only) | Yes | 2 reviewers + lead | Production |
| develop | Nobody (PR only) | Yes | 1 reviewer | Staging |
| feature/* | Developer | No | N/A | Dev (manual) |
| hotfix/* | Senior engineer | Yes | 1 reviewer (fast-track) | Production |

### CODEOWNERS

```
# .github/CODEOWNERS

# Default owners
* @data-engineering-team

# Domain-specific
src/pipelines/risk/ @risk-engineering
src/pipelines/lending/ @lending-team
src/pipelines/customer/ @customer-platform

# Governance requires senior review
governance/ @data-governance-team @security-team

# CI/CD requires platform team
.github/ @platform-team
databricks.yml @platform-team
resources/clusters/ @platform-team
```

---

## 5.4 PR Template & Review Checklist

### Pull Request Template

```markdown
## Summary
<!-- What does this PR do? Which pipeline(s) are affected? -->

## Migration Details
- **Pipeline(s):** [names]
- **Domain:** [risk/lending/customer/...]
- **Complexity:** [simple/medium/complex]
- **Wave:** [1/2/3/...]

## Checklist
- [ ] Unit tests pass locally
- [ ] Bundle validates: `databricks bundle validate`
- [ ] Follows naming conventions (catalog.domain_layer.entity)
- [ ] Secrets used (no hardcoded credentials)
- [ ] DQ expectations defined
- [ ] Governance SQL included (tags, masks if PII/MNPI)
- [ ] Monitoring SQL included
- [ ] Validation test included
- [ ] README updated (if new pattern)

## Validation Results
<!-- Paste output from validation run -->
- Row count match: ✓/✗
- Schema match: ✓/✗
- Aggregate match: ✓/✗
- SLA met: ✓/✗

## Rollback Plan
<!-- How to revert if issues found in production -->
```

---

## 5.5 Infrastructure as Code

### All infrastructure defined in YAML (no console clicks):

```yaml
# resources/clusters/streaming_cluster.yml
resources:
  clusters:
    risk_streaming:
      cluster_name: "risk-streaming-prod"
      spark_version: "14.3.x-scala2.12"
      node_type_id: "m5d.2xlarge"
      autoscale:
        min_workers: 4
        max_workers: 16
      spark_conf:
        "spark.databricks.delta.optimizeWrite.enabled": "true"
        "spark.sql.streaming.stateStore.providerClass": "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
      custom_tags:
        domain: risk
        environment: production
        cost_center: "CC-1234"
```

---

## 5.6 Quality Gates

### Deployment Quality Gate (blocks bad deployments)

```yaml
# Pre-deploy checks (run in CI)
quality_gates:
  - name: bundle_validates
    command: "databricks bundle validate --target $TARGET"
    required: true
    
  - name: unit_tests_pass
    command: "pytest tests/unit/ --tb=short"
    required: true
    min_coverage: 80
    
  - name: no_hardcoded_secrets
    command: "grep -r 'password\|secret\|token' src/ --include='*.py' | grep -v 'dbutils.secrets'"
    required: true
    expect_empty: true
    
  - name: governance_present
    command: "test $(ls governance/*.sql | wc -l) -ge $(ls src/pipelines/**/bronze/*.py | wc -l)"
    required: true
    
  - name: monitoring_present
    command: "test $(ls monitoring/*.sql | wc -l) -ge $(ls src/pipelines/**/bronze/*.py | wc -l)"
    required: true
```

---

## Deliverables Checklist

- [ ] DAB repo structure created and documented
- [ ] CI/CD pipeline configured (GitHub Actions or Azure DevOps)
- [ ] Branch strategy defined and enforced
- [ ] CODEOWNERS configured
- [ ] PR template created
- [ ] Quality gates passing
- [ ] Service principals created (staging + production)
- [ ] All teams onboarded to git workflow
