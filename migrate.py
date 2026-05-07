#!/usr/bin/env python3
"""
migrate.py — ONE-COMMAND AWS → Databricks Migration Tool

Usage:
    python migrate.py --source ./aws-sample-data-project --dest ./databricks-project
    python migrate.py --source /path/to/aws/repo --dest /path/to/output --catalog production --org acme

That's it. No code to write. No YAML to create. No notebooks to run manually.

What happens automatically:
  1. Scans source folder (finds Airflow DAGs, Step Functions, EMR scripts, Glue jobs)
  2. Parses every file → extracts pipeline metadata
  3. Builds unified inventory
  4. Auto-generates PipelineSpecs (with quality rules, governance, compute sizing)
  5. Runs factory → produces notebooks, job YAML, DQ SQL, governance SQL, tests, monitoring
  6. Writes complete Databricks Asset Bundle repo to destination
  7. Ready to deploy: `databricks bundle deploy --target dev`
"""

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(
        description="Migrate an AWS data project to Databricks Lakehouse (one command)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate.py --source ./aws-sample-data-project --dest ./databricks-project
  python migrate.py --source /data/aws-repo --dest /data/dbx-repo --catalog prod --org mycompany
  python migrate.py --source . --dest ../output --clean
        """
    )
    parser.add_argument("--source", required=True, help="Path to source AWS project folder")
    parser.add_argument("--dest", required=True, help="Path for output Databricks project folder")
    parser.add_argument("--catalog", default="production", help="Target Unity Catalog name (default: production)")
    parser.add_argument("--org", default="acme", help="Organization name (default: acme)")
    parser.add_argument("--clean", action="store_true", help="Delete destination folder before generating")
    parser.add_argument("--ai", action="store_true",
                        help="Use AI (databricks-claude-opus-4-6) for enhanced code transformation and docs")
    parser.add_argument("--ai-model", default="opus", choices=["opus", "sonnet", "haiku"],
                        help="AI model tier: opus (best), sonnet (balanced), haiku (fast)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview mode: scan and generate specs but don't write artifacts")
    parser.add_argument("--threads", type=int, default=1,
                        help="Parallel threads for AI calls (only with --ai, default: 1)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    
    args = parser.parse_args()
    
    source = os.path.abspath(args.source)
    dest = os.path.abspath(args.dest)
    
    print()
    print("═" * 70)
    print("  aws2lakehouse — AWS → Databricks Migration Tool")
    print("═" * 70)
    print(f"  Source:      {source}")
    print(f"  Destination: {dest}")
    print(f"  Catalog:     {args.catalog}")
    print(f"  Org:         {args.org}")
    print(f"  Timestamp:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 70)
    
    # Validate source exists
    if not os.path.isdir(source):
        print(f"\n  ❌ ERROR: Source directory not found: {source}")
        sys.exit(1)
    
    # Clean destination if requested
    if args.clean and os.path.exists(dest):
        shutil.rmtree(dest)
        print(f"\n  🗑️  Cleaned existing destination")
    
    # ═══ STEP 1: SCAN ═══
    print(f"\n  [1/8] Scanning source repository...")
    manifest = scan_source(source)
    total = sum(len(v) for v in manifest.values())
    print(f"        Found {total} files: {len(manifest['airflow_dags'])} DAGs, "
          f"{len(manifest['step_functions'])} Step Funcs, {len(manifest['emr_scripts'])} EMR, "
          f"{len(manifest['glue_jobs'])} Glue, {len(manifest['configs'])} configs")
    
    # ═══ STEP 2: PARSE ═══
    print(f"  [2/8] Parsing pipeline metadata...")
    inventory = build_inventory(source, manifest, args.verbose)
    print(f"        Built inventory: {len(inventory)} pipelines across "
          f"{len(set(p['domain'] for p in inventory))} domains")
    
    # ═══ STEP 3: GENERATE SPECS ═══
    print(f"  [3/8] Auto-generating pipeline specs...")
    # Add accelerator to path
    accelerator_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, accelerator_dir)
    
    from aws2lakehouse.factory.spec_generator import SpecGenerator
    from aws2lakehouse.factory import PipelineFactory
    
    generator = SpecGenerator(catalog=args.catalog)
    specs = generator.from_inventory(inventory)
    print(f"        Generated {len(specs)} PipelineSpecs (zero manual YAML)")
    
    # M7: --dry-run exits after spec generation
    if args.dry_run:
        print()
        print("═" * 70)
        print("  DRY RUN COMPLETE (no artifacts written)")
        print("═" * 70)
        print(f"  Would generate: {len(specs)} pipelines × 6 artifacts = {len(specs)*6} files")
        print(f"  Domains: {', '.join(sorted(set(p['domain'] for p in inventory)))}")
        print(f"  Source types: {', '.join(sorted(set(p['source_type'] for p in inventory)))}")
        print()
        print("  Spec summary:")
        for s in specs:
            print(f"    • {s.name} ({s.domain}/{s.source.type.value}) "
                  f"→ {s.target.catalog}.{s.target.schema}.{s.target.table}")
        print()
        print("  Run without --dry-run to generate artifacts.")
        print("═" * 70)
        return
    
    # ═══ STEP 4: RUN FACTORY ═══
    print(f"  [4/8] Running pipeline factory...")
    factory = PipelineFactory(catalog=args.catalog, environment="prod")
    all_artifacts = [factory.generate(spec) for spec in specs]
    total_chars = sum(len(a.notebook_code) + len(a.job_yaml) + len(a.dq_sql) +
                      len(a.governance_sql) + len(a.test_code) + len(a.monitoring_sql)
                      for a in all_artifacts)
    print(f"        Generated {len(all_artifacts) * 6} files ({total_chars:,} chars of code)")
    
    # ═══ STEP 5: CREATE STRUCTURE ═══
    print(f"  [5/8] Creating destination repo structure...")
    create_repo_structure(dest, inventory)
    
    # ═══ STEP 6: WRITE ARTIFACTS ═══
    print(f"  [6/8] Writing all artifacts...")
    write_artifacts(dest, all_artifacts, args.catalog, args.org)
    
    # ═══ STEP 7: WRITE SUPPORTING FILES ═══
    print(f"  [7/8] Writing bundle config, CI/CD, README...")
    write_supporting_files(dest, all_artifacts, args.catalog, args.org, inventory)
    
    # ═══ STEP 8: GENERATE DOCUMENTATION ═══
    total_steps = 9 if args.ai else 8
    print(f"  [8/{total_steps}] Generating project-specific documentation...")
    from aws2lakehouse.docs_generator import DocsGenerator
    docs_gen = DocsGenerator(
        project_name=os.path.basename(dest),
        org=args.org,
        catalog=args.catalog,
        inventory=inventory,
        artifacts=all_artifacts,
        source_path=args.source,
        dest_path=dest
    )
    doc_count = docs_gen.generate_all()
    print(f"         → {doc_count} docs generated (docs/ folder)")
    
    # ═══ STEP 9 (optional): AI ENHANCEMENT ═══
    if args.ai:
        print(f"  [9/9] AI-enhancing migration with {args.ai_model}...")
        from aws2lakehouse.genai.ai_client import AIClient
        ai = AIClient(model=args.ai_model)
        
        ai_enhancements = 0
        
        # 1. Generate executive summary
        exec_summary = ai.generate_migration_summary(
            inventory=inventory,
            source_desc=f"AWS project at {args.source}",
            dest_desc=f"Databricks Lakehouse ({args.catalog})"
        )
        Path(f"{dest}/docs/EXECUTIVE_SUMMARY.md").write_text(
            f"# Executive Summary\n\n> AI-generated by databricks-claude-{args.ai_model}\n\n{exec_summary}\n"
        )
        ai_enhancements += 1
        
        # 2. AI-transform top 3 complex pipelines (full code rewrite)
        complex_pipelines = sorted(inventory, 
                                   key=lambda p: p.get("business_impact","medium") == "critical",
                                   reverse=True)[:3]
        
        def _transform_one(p):
            """Transform a single pipeline (for threading)."""
            notebook_path = f"{dest}/src/pipelines/{p['domain']}/bronze/{p['name']}.py"
            if not os.path.exists(notebook_path):
                return None
            original_code = Path(notebook_path).read_text()
            enhanced = ai.transform_code(
                source_code=original_code,
                source_type=p.get("source_type", "emr"),
                context=f"catalog={args.catalog}, domain={p['domain']}, "
                        f"classification={p.get('classification','internal')}"
            )
            out_path = f"{dest}/src/pipelines/{p['domain']}/bronze/{p['name']}_ai_enhanced.py"
            Path(out_path).write_text(enhanced)
            return p['name']
        
        # M8: Use threads if specified
        if args.threads > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=args.threads) as executor:
                futures = {executor.submit(_transform_one, p): p for p in complex_pipelines}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            ai_enhancements += 1
                    except Exception as e:
                        p = futures[future]
                        print(f"         ⚠ Skipped AI transform for {p['name']}: {str(e)[:50]}")
        else:
            for p in complex_pipelines:
                try:
                    result = _transform_one(p)
                    if result:
                        ai_enhancements += 1
                except Exception as e:
                    print(f"         ⚠ Skipped AI transform for {p['name']}: {str(e)[:50]}")
        
        # 3. Generate pipeline descriptions for README
        descriptions = []
        for p in inventory[:5]:
            try:
                desc = ai.explain_pipeline(p)
                descriptions.append(f"### {p['name']}\n{desc}\n")
            except Exception as e:
                print(f"         ⚠ Skipped description for {p['name']}: {str(e)[:50]}")
        
        if descriptions:
            Path(f"{dest}/docs/PIPELINE_DESCRIPTIONS.md").write_text(
                f"# Pipeline Descriptions\n\n> AI-generated natural language descriptions\n\n" +
                "\n".join(descriptions)
            )
            ai_enhancements += 1
        
        # 4. Enrich the migration report with AI insights
        report_path = f"{dest}/docs/MIGRATION_REPORT.md"
        if os.path.exists(report_path):
            try:
                report = Path(report_path).read_text()
                enriched = ai.enrich_docs(report, 
                    f"Migration of {len(inventory)} pipelines from AWS to Databricks "
                    f"for {args.org} (financial services, regulated)")
                Path(f"{dest}/docs/MIGRATION_REPORT_ENRICHED.md").write_text(enriched)
                ai_enhancements += 1
            except Exception as e:
                print(f"         ⚠ Skipped report enrichment: {str(e)[:60]}")
        
        print(f"         → {ai_enhancements} AI enhancements applied")
        print(f"           (executive summary, code transforms, pipeline descriptions, enriched docs)")
        if hasattr(ai, '_total_tokens') and ai._total_tokens > 0:
            cost_est = ai._total_tokens * 0.00001  # rough estimate
            print(f"         → {ai._total_tokens:,} tokens used (est. ${cost_est:.2f})")
    
    # ═══ DONE ═══
    file_count = sum(len(files) for _, _, files in os.walk(dest))
    total_size = sum(os.path.getsize(os.path.join(r, f)) 
                     for r, _, files in os.walk(dest) for f in files)
    
    print()
    print("═" * 70)
    print(f"  ✅ MIGRATION COMPLETE")
    print(f"═" * 70)
    print(f"  Output: {dest}")
    print(f"  Files:  {file_count} files ({total_size:,} bytes)")
    print(f"")
    print(f"  Next steps:")
    print(f"    cd {dest}")
    print(f"    databricks bundle validate --target dev")
    print(f"    databricks bundle deploy --target dev")
    print("═" * 70)
    print()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def scan_source(source_dir):
    """Scan source directory and categorize files.
    
    M6: Uses both path keywords AND file content to detect DAGs.
    A .py file with DAG() instantiation is an Airflow DAG regardless of path.
    """
    manifest = {"airflow_dags": [], "step_functions": [], "emr_scripts": [],
                "glue_jobs": [], "configs": [], "schemas": [], "other": []}
    
    for root, dirs, files in os.walk(source_dir):
        for f in files:
            filepath = os.path.join(root, f)
            rel = os.path.relpath(filepath, source_dir)
            size = os.path.getsize(filepath)
            entry = {"file": f, "path": rel, "full_path": filepath, "size": size}
            
            if "airflow" in rel and "dag" in rel and f.endswith(".py"):
                manifest["airflow_dags"].append(entry)
            elif "step_function" in rel and f.endswith(".json"):
                manifest["step_functions"].append(entry)
            elif "emr" in rel and f.endswith(".sh"):
                manifest["emr_scripts"].append(entry)
            elif "glue" in rel and f.endswith(".py"):
                manifest["glue_jobs"].append(entry)
            elif f.endswith(".json") and "config" in rel:
                manifest["configs"].append(entry)
            elif f.endswith(".json") and "schema" in rel:
                manifest["schemas"].append(entry)
            elif f.endswith(".py") and size < 500_000:
                # M6: Content-based detection for .py files not caught by path
                try:
                    with open(filepath, "r", errors="ignore") as fh:
                        head = fh.read(5000)  # Read first 5KB only
                    if re.search(r"DAG\(", head) and "airflow" in head.lower():
                        manifest["airflow_dags"].append(entry)
                    elif "GlueContext" in head or "awsglue" in head:
                        manifest["glue_jobs"].append(entry)
                    else:
                        manifest["other"].append(entry)
                except (IOError, UnicodeDecodeError):
                    manifest["other"].append(entry)
            else:
                manifest["other"].append(entry)
    
    return manifest


def build_inventory(source_dir, manifest, verbose=False):
    """Parse all source files and build unified pipeline inventory."""
    inventory = []
    
    # Parse Airflow DAGs
    fallback_count = 0
    for item in manifest["airflow_dags"]:
        with open(item["full_path"]) as f:
            content = f.read()
        
        dag_id = _extract(r'DAG\(\s*"([^"]+)"', content)
        if not dag_id:
            dag_id = item["file"].replace(".py", "")
            fallback_count += 1
            if verbose:
                print(f"        ⚠ {item['file']}: DAG ID not parsed, using filename")
        schedule = _extract(r'schedule_interval\s*=\s*"([^"]+)"', content) or "0 6 * * *"
        owner = _extract(r'"owner":\s*"([^"]+)"', content) or "data-team"
        
        # Detect source type from operators
        if "EmrAddStepsOperator" in content:
            source_type = "delta_table" if "S3KeySensor" in content else "kafka"
        elif "GlueJobOperator" in content:
            if "mongodb" in content.lower():
                source_type = "mongodb"
            elif "jdbc" in content.lower() or "postgres" in content.lower():
                source_type = "jdbc"
            else:
                source_type = "auto_loader"
        else:
            source_type = "auto_loader"
        
        # Detect classification from tags
        tags_match = re.findall(r'tags\s*=\s*\[([^\]]+)\]', content)
        tags = tags_match[0] if tags_match else ""
        classification = "mnpi" if "mnpi" in tags else "confidential" if "pii" in tags else "internal"
        
        domain = _extract_domain(tags, dag_id)
        
        inventory.append({
            "name": dag_id,
            "domain": domain,
            "source_type": source_type,
            "source_config": {},
            "schedule": schedule,
            "sla_minutes": 60,
            "business_impact": "critical" if "critical" in tags else "high",
            "owner": f"{owner}@company.com",
            "mnpi_columns": [],
            "pii_columns": [],
            "classification": classification,
            "num_executors": 10,
            "tables_written": [],
            "tables_read": [],
        })
    
    if fallback_count and verbose:
        print(f"        ℹ {fallback_count} DAGs used filename fallback for ID")
    
    # Parse EMR scripts
    for item in manifest["emr_scripts"]:
        with open(item["full_path"]) as f:
            content = f.read()
        
        name = item["file"].replace(".sh", "")
        topic = _extract(r'--topic\s+(\S+)', content)
        is_streaming = "--checkpoint" in content
        executors = int(_extract(r'--num-executors\s+(\d+)', content) or "4")
        
        source_type = "kafka" if topic else "delta_table"
        schedule = "continuous" if is_streaming and "30 second" in content else "*/5 * * * *" if is_streaming else "0 2 * * 0"
        
        inventory.append({
            "name": name,
            "domain": "risk" if "trade" in name or "market" in name else "analytics",
            "source_type": source_type,
            "source_config": {"topic": topic} if topic else {},
            "schedule": schedule,
            "sla_minutes": 5 if is_streaming else 240,
            "business_impact": "critical" if is_streaming else "medium",
            "owner": "data-team@company.com",
            "mnpi_columns": [],
            "pii_columns": [],
            "classification": "mnpi" if "trade" in name or "market" in name else "internal",
            "num_executors": executors,
            "tables_written": [],
            "tables_read": [],
        })
    
    # Parse Glue jobs
    for item in manifest["glue_jobs"]:
        with open(item["full_path"]) as f:
            content = f.read()
        
        name = item["file"].replace(".py", "")
        has_jdbc = "jdbc" in content.lower() or "postgres" in content.lower()
        has_pii = "mask" in content.lower() or "account_number" in content.lower() or "ssn" in content.lower()
        
        source_type = "jdbc" if has_jdbc else "auto_loader"
        
        inventory.append({
            "name": name,
            "domain": "lending" if "payment" in name or "loan" in name else "finance",
            "source_type": source_type,
            "source_config": {},
            "schedule": "*/15 * * * *" if "payment" in name else "triggered",
            "sla_minutes": 10 if "payment" in name else 60,
            "business_impact": "high" if has_pii else "medium",
            "owner": "data-team@company.com",
            "mnpi_columns": [],
            "pii_columns": ["account_number"] if "payment" in name else [],
            "classification": "confidential" if has_pii else "internal",
            "tables_written": [],
            "tables_read": [],
        })
    
    return inventory


def create_repo_structure(dest, inventory):
    """Create the DAB directory structure."""
    dirs = ["src/common", "resources/jobs", "governance/column_masks",
            "quality", "monitoring", "tests/validation", "docs", ".github/workflows"]
    
    for domain in set(p["domain"] for p in inventory):
        dirs.append(f"src/pipelines/{domain}/bronze")
    
    for d in dirs:
        os.makedirs(os.path.join(dest, d), exist_ok=True)


def write_artifacts(dest, all_artifacts, catalog, org):
    """Write all generated artifacts to the destination repo."""
    for art in all_artifacts:
        domain = art.spec.domain
        name = art.spec.name
        
        # Notebooks
        nb_path = os.path.join(dest, f"src/pipelines/{domain}/bronze/{name}.py")
        Path(nb_path).write_text(art.notebook_code)
        
        # Job YAML
        Path(os.path.join(dest, f"resources/jobs/{name}.yml")).write_text(art.job_yaml)
        
        # DQ SQL
        Path(os.path.join(dest, f"quality/{name}_expectations.sql")).write_text(art.dq_sql)
        
        # Governance SQL
        Path(os.path.join(dest, f"governance/column_masks/{name}.sql")).write_text(art.governance_sql)
        
        # Tests
        Path(os.path.join(dest, f"tests/validation/test_{name}.py")).write_text(art.test_code)
        
        # Monitoring
        Path(os.path.join(dest, f"monitoring/{name}_monitoring.sql")).write_text(art.monitoring_sql)


def write_supporting_files(dest, all_artifacts, catalog, org, inventory):
    """Write databricks.yml, README, CI/CD, .gitignore."""
    
    # databricks.yml
    job_includes = "\n".join(f"    - resources/jobs/{a.spec.name}.yml" for a in all_artifacts)
    bundle_yml = f"""bundle:
  name: {org}-data-platform

workspace:
  host: ${{var.workspace_host}}

variables:
  workspace_host:
    description: Databricks workspace URL

targets:
  dev:
    mode: development
    default: true
    variables:
      workspace_host: ""

  staging:
    mode: development
    variables:
      workspace_host: ""

  production:
    mode: production
    variables:
      workspace_host: ""
    run_as:
      service_principal_name: {org}-data-platform-sp

include:
{job_includes}
"""
    Path(os.path.join(dest, "databricks.yml")).write_text(bundle_yml)
    
    # .gitignore
    gitignore = """.databricks/
.bundle/
__pycache__/
*.pyc
.venv/
.DS_Store
"""
    Path(os.path.join(dest, ".gitignore")).write_text(gitignore)
    
    # GitHub Actions
    ci_yml = """name: Deploy
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate
      - run: databricks bundle deploy --target production
"""
    Path(os.path.join(dest, ".github/workflows/deploy.yml")).write_text(ci_yml)
    
    # README
    pipeline_table = "\n".join(
        f"| {a.spec.name} | {a.spec.domain} | {a.spec.source.type.value} | {a.spec.governance.classification.value} |"
        for a in all_artifacts
    )
    readme = f"""# {org.title()} Data Platform (Databricks)

Auto-generated from AWS source project using **aws2lakehouse** accelerator.

## Deploy

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

## Pipelines ({len(all_artifacts)})

| Pipeline | Domain | Source | Classification |
|----------|--------|--------|----------------|
{pipeline_table}

## Structure

- `src/pipelines/` — Ingestion notebooks (per domain)
- `resources/jobs/` — Job schedules and compute config
- `governance/` — Column masks, row filters, bootstrap SQL
- `quality/` — Data quality expectations
- `monitoring/` — Freshness SLA and volume anomaly queries
- `tests/` — Migration validation tests
"""
    Path(os.path.join(dest, "README.md")).write_text(readme)


def _extract(pattern, text):
    m = re.search(pattern, text)
    return m.group(1) if m else None


def _extract_domain(tags_str, name, domain_mapping=None):
    """Extract domain from tags/name. Accepts optional custom mapping.
    
    Args:
        domain_mapping: Dict of {keyword: domain} for custom domains.
                        e.g. {"treasury": "treasury", "fx": "markets"}
    """
    text = f"{tags_str} {name}".lower()
    
    # Check custom mapping first (if provided)
    if domain_mapping:
        for keyword, domain in domain_mapping.items():
            if keyword in text:
                return domain
    
    # Default mapping
    default_map = {
        "risk": "risk", "trade": "risk", "market": "risk",
        "lending": "lending", "loan": "lending", "mortgage": "lending",
        "customer": "customer", "user": "customer", "account": "customer",
        "compliance": "compliance", "audit": "compliance", "regulatory": "compliance",
        "finance": "finance", "vendor": "finance", "partner": "finance",
        "payment": "finance", "invoice": "finance",
    }
    for keyword, domain in default_map.items():
        if keyword in text:
            return domain
    
    return "analytics"


if __name__ == "__main__":
    main()
