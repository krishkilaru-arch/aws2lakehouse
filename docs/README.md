# Migration Documentation

## Complete Methodology (6 Phases)

This documentation covers the end-to-end AWS → Databricks Lakehouse migration methodology, aligned with enterprise-grade requirements for financial services (MNPI, PII, SOX compliance).

| # | Document | Scope | Key Deliverables |
|---|----------|-------|-----------------|
| 1 | [Strategic Planning & Discovery](01_STRATEGIC_PLANNING_AND_DISCOVERY.md) | What exists, who owns it, in what order | Inventory, complexity scores, wave plan |
| 2 | [Pilot & Ingestion Design](02_PILOT_AND_INGESTION_DESIGN.md) | Prove it works, define patterns | 6 ingestion patterns, medallion arch, orchestration map |
| 3 | [Target Architecture & Governance](03_TARGET_ARCHITECTURE_AND_GOVERNANCE.md) | Design the destination | Unity Catalog, MNPI controls, DQ framework, DR strategy |
| 4 | [Migration Execution](04_MIGRATION_EXECUTION.md) | Move 1000 pipelines | Compute mapping, code transforms, config framework, validation |
| 5 | [CI/CD & DevOps](05_CICD_AND_DEVOPS.md) | Deploy safely at scale | DABs, GitHub Actions, branching, quality gates, IaC |
| 6 | [Collaboration & Improvement](06_COLLABORATION_AND_IMPROVEMENT.md) | Sustain velocity, optimize | RACI, metrics, compute optimization, ADRs, handoff |

## Additional Documentation

| Document | Purpose |
|----------|---------|
| [QUICKSTART.md](QUICKSTART.md) | 5-minute demo of the accelerator |
| [MIGRATION_PLAYBOOK.md](MIGRATION_PLAYBOOK.md) | Phase-by-phase execution guide |
| [PIPELINE_YAML_REFERENCE.md](PIPELINE_YAML_REFERENCE.md) | Complete YAML spec reference |
| [MODULE_REFERENCE.md](MODULE_REFERENCE.md) | API docs for all modules |
| [STAKEHOLDER_TEMPLATES.md](STAKEHOLDER_TEMPLATES.md) | Interview scripts, RACI, risk register |
| [CUTOVER_PLAYBOOK.md](CUTOVER_PLAYBOOK.md) | Dual-write, shadow mode, big-bang strategies |

## How Documentation Relates to the Tool

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  HUMAN READS DOCS              TOOL AUTOMATES                              │
│  ─────────────────             ───────────────                             │
│                                                                             │
│  Doc 1: Planning          →    Scanners, ComplexityAnalyzer, WavePlanner    │
│  Doc 2: Ingestion         →    ConnectorFactory, all 7 source patterns     │
│  Doc 3: Governance        →    Bootstrap, MNPIController, MonitoringDash   │
│  Doc 4: Execution         →    SpecGenerator, PipelineFactory, Validator   │
│  Doc 5: CI/CD             →    DABGenerator, BranchStrategy                │
│  Doc 6: Collaboration     →    ROICalculator, system table queries         │
│                                                                             │
│  The docs explain the WHY.                                                  │
│  The tool automates the HOW.                                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Target Audience

| Audience | Start Here | Key Sections |
|----------|-----------|--------------|
| Migration Lead / Architect | Doc 1 → Doc 3 | Planning, architecture, governance |
| Data Engineer | Doc 2 → Doc 4 | Ingestion patterns, code transforms |
| DevOps / Platform | Doc 5 | CI/CD, DABs, IaC |
| Engineering Manager | Doc 6 | Metrics, RACI, velocity tracking |
| Compliance / Security | Doc 3 (§3.3, §3.4) | MNPI controls, DQ framework |
| Executive Sponsor | Doc 1 (§1.6) + Doc 6 (§6.4) | Success metrics, health dashboard |
