# Quick Start Guide

## Installation

```bash
# Clone the repository
git clone <repo-url> aws2lakehouse
cd aws2lakehouse

# Install with all dependencies
pip install -e ".[aws,databricks,dev]"

# Or minimal install (no cloud SDKs)
pip install -e .
```

## Prerequisites

| Requirement | Purpose | Required? |
|-------------|---------|-----------|
| Python 3.10+ | Runtime | Yes |
| pyyaml | Pipeline spec parsing | Yes |
| pydantic | Config validation | Yes |
| boto3 | AWS scanning (EMR/Glue/SF) | For discovery only |
| databricks-sdk | Databricks API calls | For deployment only |
| mlflow | GenAI features | Optional |

## 5-Minute Demo

### Step 1: Generate a pipeline from natural language

```python
from aws2lakehouse.genai import PipelineGenerator

gen = PipelineGenerator()
yaml_spec = gen.from_description(
    "Ingest daily trade events from Kafka topic 'trade-events' "
    "into Bronze table with MNPI classification, partition by trade_date, "
    "alert on failures via Slack"
)
print(yaml_spec)

# Save to file
with open("pipelines/trade_events.yaml", "w") as f:
    f.write(yaml_spec)
```

### Step 2: Generate all artifacts from the spec

```python
from aws2lakehouse.factory import PipelineSpec, PipelineFactory

# Load the spec
spec = PipelineSpec.from_yaml("pipelines/trade_events.yaml")

# Generate everything
factory = PipelineFactory(catalog="production", environment="prod")
artifacts = factory.generate(spec)

# What you get:
print(f"Notebook:   {len(artifacts.notebook_code)} chars")
print(f"Job YAML:   {len(artifacts.job_yaml)} chars")
print(f"DQ SQL:     {len(artifacts.dq_sql)} chars")
print(f"Governance: {len(artifacts.governance_sql)} chars")
print(f"Tests:      {len(artifacts.test_code)} chars")
print(f"Monitoring: {len(artifacts.monitoring_sql)} chars")
```

### Step 3: Deploy to Databricks

```python
# Write notebook to workspace
import json

# Save as Databricks notebook
with open(f"src/pipelines/{spec.domain}/{spec.name}.py", "w") as f:
    f.write(artifacts.notebook_code)

# Save job definition for DAB
with open(f"resources/{spec.name}.yml", "w") as f:
    f.write(artifacts.job_yaml)

# Deploy with Databricks Asset Bundles
# $ databricks bundle deploy -t production
```

## Batch Migration (1000 Pipelines)

```python
from aws2lakehouse.factory import PipelineFactory
from pathlib import Path

factory = PipelineFactory(catalog="production", environment="prod")

# Generate artifacts for ALL pipeline specs in a directory
all_artifacts = factory.generate_batch("pipelines/")

# Write all notebooks and job YAMLs
for artifact in all_artifacts:
    spec = artifact.spec
    out_dir = Path(f"output/{spec.domain}")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    (out_dir / f"{spec.name}.py").write_text(artifact.notebook_code)
    (out_dir / f"{spec.name}.yml").write_text(artifact.job_yaml)
    (out_dir / f"{spec.name}_dq.sql").write_text(artifact.dq_sql)
    (out_dir / f"{spec.name}_gov.sql").write_text(artifact.governance_sql)
    (out_dir / f"{spec.name}_test.py").write_text(artifact.test_code)

print(f"Generated {len(all_artifacts)} pipeline artifacts")
```

## What's Next?

- [Migration Playbook](MIGRATION_PLAYBOOK.md) — Full 6-phase methodology
- [Pipeline YAML Reference](PIPELINE_YAML_REFERENCE.md) — Complete spec format
- [Module Reference](MODULE_REFERENCE.md) — All classes and methods
