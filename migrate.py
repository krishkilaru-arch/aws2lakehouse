#!/usr/bin/env python3
"""
migrate.py — ONE-COMMAND AWS → Databricks Migration Tool

Usage:
    python migrate.py --source ./aws-sample-data-project --dest ./databricks-project
    python migrate.py --source /path/to/aws/repo --dest /path/to/output --catalog production --org acme

What happens automatically:
  1. Scans source folder (finds Airflow DAGs, Step Functions, EMR scripts, Glue jobs)
  2. Parses every file → extracts pipeline metadata (AST-based for Python files)
  3. Builds unified inventory
  4. Auto-generates PipelineSpecs (with quality rules, governance, compute sizing)
  5. Runs factory → produces notebooks, job YAML, DQ SQL, governance SQL, tests, monitoring
  6. Writes complete Databricks Asset Bundle repo to destination
  7. Saves migration state for resume support
  8. Ready to deploy: `databricks bundle deploy --target dev`
"""

import argparse
import ast
import contextlib
import json
import logging
import os
import re
import shutil
import sys
import traceback
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("aws2lakehouse.migrate")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate an AWS data project to Databricks Lakehouse (one command)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate.py --source ./aws-sample-data-project --dest ./databricks-project
  python migrate.py --source /data/aws-repo --dest /data/dbx-repo --catalog prod --org mycompany
  python migrate.py --source . --dest ../output --clean
  python migrate.py --source ./repo --dest ./out --resume   # resume after failure
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
    parser.add_argument("--resume", action="store_true",
                        help="Resume a previously interrupted migration run")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")

    source = os.path.abspath(args.source)
    dest = os.path.abspath(args.dest)

    print()
    print("=" * 70)
    print("  aws2lakehouse -- AWS -> Databricks Migration Tool")
    print("=" * 70)
    print(f"  Source:      {source}")
    print(f"  Destination: {dest}")
    print(f"  Catalog:     {args.catalog}")
    print(f"  Org:         {args.org}")
    print(f"  Timestamp:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.resume:
        print("  Mode:        RESUME (will skip completed steps)")
    print("=" * 70)

    # Validate source exists
    if not os.path.isdir(source):
        print(f"\n  ERROR: Source directory not found: {source}")
        sys.exit(1)

    # ── State management ────────────────────────────────────────────────────
    accelerator_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, accelerator_dir)
    from aws2lakehouse.state import MigrationState

    state = None
    if args.resume:
        state = MigrationState.load(dest)
        if state:
            print(f"\n  Resuming run {state.run_id} "
                  f"({state.completed_pipelines}/{state.total_pipelines} pipelines done)")
        else:
            print("\n  No previous state found — starting fresh.")

    if state is None:
        # Clean destination if requested (only on fresh runs)
        if args.clean and os.path.exists(dest):
            shutil.rmtree(dest)
            print("\n  Cleaned existing destination")
        state = MigrationState.create(source, dest, args.catalog, args.org)

    # ═══ STEP 1: SCAN ═══
    if not state.is_step_completed("scan"):
        state.mark_step_started("scan")
        print("\n  [1/8] Scanning source repository...")
        manifest = scan_source(source)
        total = sum(len(v) for v in manifest.values())
        print(f"        Found {total} files: {len(manifest['airflow_dags'])} DAGs, "
              f"{len(manifest['step_functions'])} Step Funcs, {len(manifest['emr_scripts'])} EMR, "
              f"{len(manifest['glue_jobs'])} Glue, {len(manifest['configs'])} configs")
        # Cache manifest for resume
        os.makedirs(dest, exist_ok=True)
        Path(os.path.join(dest, ".manifest.json")).write_text(
            json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        state.mark_step_completed("scan")
    else:
        print("\n  [1/8] Scanning... (cached)")
        manifest = json.loads(Path(os.path.join(dest, ".manifest.json")).read_text(encoding="utf-8"))

    # ═══ STEP 2: PARSE ═══
    if not state.is_step_completed("parse"):
        state.mark_step_started("parse")
        print("  [2/8] Parsing pipeline metadata...")
        inventory = build_inventory(source, manifest, args.verbose)
        print(f"        Built inventory: {len(inventory)} pipelines across "
              f"{len(set(p['domain'] for p in inventory))} domains")
        Path(os.path.join(dest, ".inventory.json")).write_text(
            json.dumps(inventory, indent=2, default=str), encoding="utf-8")
        state.total_pipelines = len(inventory)
        state.mark_step_completed("parse")
    else:
        print("  [2/8] Parsing... (cached)")
        inventory = json.loads(Path(os.path.join(dest, ".inventory.json")).read_text(encoding="utf-8"))

    # ═══ STEP 3: GENERATE SPECS ═══
    print("  [3/8] Auto-generating pipeline specs...")
    from aws2lakehouse.factory import PipelineFactory
    from aws2lakehouse.factory.spec_generator import SpecGenerator

    generator = SpecGenerator(catalog=args.catalog)
    specs = generator.from_inventory(inventory)
    print(f"        Generated {len(specs)} PipelineSpecs (zero manual YAML)")

    # --dry-run exits after spec generation
    if args.dry_run:
        print()
        print("=" * 70)
        print("  DRY RUN COMPLETE (no artifacts written)")
        print("=" * 70)
        print(f"  Would generate: {len(specs)} pipelines x 6 artifacts = {len(specs)*6} files")
        print(f"  Domains: {', '.join(sorted(set(p['domain'] for p in inventory)))}")
        print(f"  Source types: {', '.join(sorted(set(p['source_type'] for p in inventory)))}")
        print()
        print("  Spec summary:")
        for s in specs:
            print(f"    - {s.name} ({s.domain}/{s.source.type.value}) "
                  f"-> {s.target.catalog}.{s.target.schema}.{s.target.table}")
        print()
        print("  Run without --dry-run to generate artifacts.")
        print("=" * 70)
        return

    # ═══ STEP 4: RUN FACTORY (per-pipeline with error isolation) ═══
    if not state.is_step_completed("factory"):
        state.mark_step_started("factory")
        print("  [4/8] Running pipeline factory...")
        factory = PipelineFactory(catalog=args.catalog, environment="prod")
        all_artifacts = []
        for spec in specs:
            if state.is_pipeline_completed(spec.name):
                # Already done in a previous run — regenerate artifact in memory for later steps
                with contextlib.suppress(Exception):
                    all_artifacts.append(factory.generate(spec))
                continue
            state.mark_pipeline_started(spec.name, spec.domain)
            try:
                art = factory.generate(spec)
                all_artifacts.append(art)
                state.mark_pipeline_completed(spec.name, [
                    f"src/pipelines/{spec.domain}/bronze/{spec.name}.py",
                    f"resources/jobs/{spec.name}.yml",
                    f"quality/{spec.name}_expectations.sql",
                    f"governance/column_masks/{spec.name}.sql",
                    f"tests/validation/test_{spec.name}.py",
                    f"monitoring/{spec.name}_monitoring.sql",
                ])
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                state.mark_pipeline_failed(spec.name, msg)
                logger.warning(f"Factory failed for {spec.name}: {msg}")
                if args.verbose:
                    traceback.print_exc()

        total_chars = sum(len(a.notebook_code) + len(a.job_yaml) + len(a.dq_sql) +
                          len(a.governance_sql) + len(a.test_code) + len(a.monitoring_sql)
                          for a in all_artifacts)
        print(f"        Generated {len(all_artifacts) * 6} files ({total_chars:,} chars of code)")
        if state.failed_pipelines:
            print(f"        WARNING: {state.failed_pipelines} pipeline(s) failed (see state file)")
        state.mark_step_completed("factory")
    else:
        print("  [4/8] Factory... (cached)")
        factory = PipelineFactory(catalog=args.catalog, environment="prod")
        all_artifacts = []
        for spec in specs:
            with contextlib.suppress(Exception):
                all_artifacts.append(factory.generate(spec))

    # ═══ STEP 5: CREATE STRUCTURE ═══
    print("  [5/8] Creating destination repo structure...")
    create_repo_structure(dest, inventory)

    # ═══ STEP 6: WRITE ARTIFACTS ═══
    if not state.is_step_completed("write"):
        state.mark_step_started("write")
        print("  [6/8] Writing all artifacts...")
        write_artifacts(dest, all_artifacts, args.catalog, args.org)
        state.mark_step_completed("write")
    else:
        print("  [6/8] Writing... (cached)")

    # ═══ STEP 7: WRITE SUPPORTING FILES ═══
    if not state.is_step_completed("supporting"):
        state.mark_step_started("supporting")
        print("  [7/8] Writing bundle config, CI/CD, README...")
        write_supporting_files(dest, all_artifacts, args.catalog, args.org, inventory)
        state.mark_step_completed("supporting")
    else:
        print("  [7/8] Supporting files... (cached)")

    # ═══ STEP 8: GENERATE DOCUMENTATION ═══
    total_steps = 9 if args.ai else 8
    if not state.is_step_completed("docs"):
        state.mark_step_started("docs")
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
        print(f"         -> {doc_count} docs generated (docs/ folder)")
        state.mark_step_completed("docs")
    else:
        print(f"  [8/{total_steps}] Docs... (cached)")

    # ═══ STEP 9 (optional): AI ENHANCEMENT ═══
    if args.ai and not state.is_step_completed("ai"):
        state.mark_step_started("ai")
        print(f"  [9/9] AI-enhancing migration with {args.ai_model}...")
        try:
            from aws2lakehouse.genai.ai_client import AIClient
            ai = AIClient(model=args.ai_model)
        except RuntimeError as e:
            print(f"         WARNING: AI unavailable ({e}). Skipping AI step.")
            state.mark_step_completed("ai")
            ai = None

        if ai is not None:
            ai_enhancements = 0

            # 1. Generate executive summary
            try:
                exec_summary = ai.generate_migration_summary(
                    inventory=inventory,
                    source_desc=f"AWS project at {args.source}",
                    dest_desc=f"Databricks Lakehouse ({args.catalog})"
                )
                Path(f"{dest}/docs/EXECUTIVE_SUMMARY.md").write_text(
                    f"# Executive Summary\n\n> AI-generated by databricks-claude-{args.ai_model}\n\n{exec_summary}\n"
                )
                ai_enhancements += 1
            except Exception as e:
                logger.warning(f"AI executive summary failed: {e}")

            # 2. AI-transform top 3 complex pipelines
            complex_pipelines = sorted(inventory,
                                       key=lambda p: p.get("business_impact", "medium") == "critical",
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
                            f"classification={p.get('classification', 'internal')}"
                )
                out_path = f"{dest}/src/pipelines/{p['domain']}/bronze/{p['name']}_ai_enhanced.py"
                Path(out_path).write_text(enhanced)
                return p['name']

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
                            logger.warning(f"AI transform for {p['name']}: {e}")
            else:
                for p in complex_pipelines:
                    try:
                        result = _transform_one(p)
                        if result:
                            ai_enhancements += 1
                    except Exception as e:
                        logger.warning(f"AI transform for {p['name']}: {e}")

            # 3. Generate pipeline descriptions
            descriptions = []
            for p in inventory[:5]:
                try:
                    desc = ai.explain_pipeline(p)
                    descriptions.append(f"### {p['name']}\n{desc}\n")
                except Exception as e:
                    logger.warning(f"AI description for {p['name']}: {e}")

            if descriptions:
                Path(f"{dest}/docs/PIPELINE_DESCRIPTIONS.md").write_text(
                    "# Pipeline Descriptions\n\n> AI-generated natural language descriptions\n\n" +
                    "\n".join(descriptions)
                )
                ai_enhancements += 1

            # 4. Enrich migration report
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
                    logger.warning(f"AI report enrichment: {e}")

            print(f"         -> {ai_enhancements} AI enhancements applied")
            if hasattr(ai, '_total_tokens') and ai._total_tokens > 0:
                cost_est = ai._total_tokens * 0.00001
                print(f"         -> {ai._total_tokens:,} tokens used (est. ${cost_est:.2f})")
            state.mark_step_completed("ai")

    # ═══ DONE ═══
    state.mark_complete()
    file_count = sum(len(files) for _, _, files in os.walk(dest))
    total_size = sum(os.path.getsize(os.path.join(r, f))
                     for r, _, files in os.walk(dest) for f in files)

    print()
    print("=" * 70)
    if state.failed_pipelines:
        print(f"  MIGRATION COMPLETE (with {state.failed_pipelines} warnings)")
    else:
        print("  MIGRATION COMPLETE")
    print("=" * 70)
    print(f"  Run ID:  {state.run_id}")
    print(f"  Output:  {dest}")
    print(f"  Files:   {file_count} files ({total_size:,} bytes)")
    print(f"  Success: {state.completed_pipelines}/{state.total_pipelines} pipelines")
    if state.failed_pipelines:
        print(f"  Failed:  {state.failed_pipelines} (see {state.state_file})")
    print()
    print("  Next steps:")
    print(f"    cd {dest}")
    print("    databricks bundle validate --target dev")
    print("    databricks bundle deploy --target dev")
    print("=" * 70)
    print()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def scan_source(source_dir):
    """Scan source directory and categorize files.

    Detection strategy (layered):
      1. Path-based keywords (fast, high confidence for standard layouts)
      2. AST-based analysis for .py files (detects DAGs/Glue regardless of path)
      3. Content-based fallback for files that fail AST parsing
      4. JSON structure analysis for Step Functions
    """
    manifest = {"airflow_dags": [], "step_functions": [], "emr_scripts": [],
                "glue_jobs": [], "configs": [], "schemas": [], "sql": [], "other": []}

    # Directories to skip
    skip_dirs = {".git", ".venv", "venv", "__pycache__", "node_modules", ".tox", ".eggs"}

    for root, dirs, files in os.walk(source_dir):
        # Prune ignored directories in-place
        dirs[:] = [d for d in dirs if d not in skip_dirs]

        for f in files:
            filepath = os.path.join(root, f)
            rel = os.path.relpath(filepath, source_dir)
            rel_lower = rel.lower().replace("\\", "/")

            try:
                size = os.path.getsize(filepath)
            except OSError:
                continue
            entry = {"file": f, "path": rel, "full_path": filepath, "size": size}

            # ── Fast path: unambiguous path-based matching ──────────────
            if f.endswith(".py") and _path_matches(rel_lower, ["airflow", "dag"]):
                manifest["airflow_dags"].append(entry)
            elif f.endswith(".py") and _path_matches(rel_lower, ["emr"]) and "job" in rel_lower or f.endswith(".sh") and "emr" in rel_lower:
                manifest["emr_scripts"].append(entry)
            elif f.endswith(".py") and _path_matches(rel_lower, ["glue"]):
                manifest["glue_jobs"].append(entry)
            elif f.endswith(".json") and "step_function" in rel_lower:
                # Validate it's actually a state machine definition
                if _is_step_function_json(filepath):
                    manifest["step_functions"].append(entry)
                else:
                    manifest["configs"].append(entry)
            elif f.endswith(".json") and "config" in rel_lower:
                manifest["configs"].append(entry)
            elif f.endswith(".json") and "schema" in rel_lower:
                manifest["schemas"].append(entry)
            elif f.endswith(".sql"):
                manifest["sql"].append(entry)
            elif f.endswith(".py") and size < 500_000:
                # ── AST + content-based detection ───────────────────────
                detected = _detect_python_file_type(filepath)
                if detected == "airflow_dag":
                    manifest["airflow_dags"].append(entry)
                elif detected == "glue_job":
                    manifest["glue_jobs"].append(entry)
                elif detected == "emr_script":
                    manifest["emr_scripts"].append(entry)
                else:
                    manifest["other"].append(entry)
            elif f.endswith(".json") and size < 500_000:
                # Try detecting Step Function JSON by structure
                if _is_step_function_json(filepath):
                    manifest["step_functions"].append(entry)
                else:
                    manifest["other"].append(entry)
            else:
                manifest["other"].append(entry)

    return manifest


def _path_matches(rel_lower: str, keywords: list) -> bool:
    """Check if all keywords appear in the relative path."""
    return all(kw in rel_lower for kw in keywords)


def _is_step_function_json(filepath: str) -> bool:
    """Check if a JSON file is an AWS Step Functions state machine definition."""
    try:
        with open(filepath, errors="ignore") as fh:
            head = fh.read(2000)
        # Step Functions always have "States" and usually "StartAt"
        return '"States"' in head and ('"StartAt"' in head or '"Type"' in head)
    except (OSError, UnicodeDecodeError):
        return False


def _detect_python_file_type(filepath: str) -> str:
    """Use AST analysis + content fallback to classify a Python file.

    Returns: 'airflow_dag', 'glue_job', 'emr_script', or 'unknown'
    """
    try:
        with open(filepath, errors="ignore") as fh:
            content = fh.read(10_000)  # First 10KB
    except (OSError, UnicodeDecodeError):
        return "unknown"

    # Try AST-based analysis first (more reliable than regex)
    try:
        tree = ast.parse(content)
        imports = set()
        calls = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    calls.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    calls.add(node.func.attr)

        # Airflow: imports from airflow + DAG instantiation
        airflow_imports = any(m.startswith("airflow") for m in imports)
        has_dag = "DAG" in calls
        if airflow_imports and has_dag:
            return "airflow_dag"

        # Glue: imports from awsglue
        glue_imports = any(m.startswith("awsglue") for m in imports)
        if glue_imports or "GlueContext" in calls:
            return "glue_job"

        # EMR/Spark: has SparkSession + typical EMR patterns
        spark_imports = any(m.startswith("pyspark") for m in imports)
        if spark_imports and any(kw in content for kw in ["spark-submit", "EMR", "s3://", "hadoop"]):
            return "emr_script"

    except SyntaxError:
        pass  # Fall through to content-based detection

    # Content-based fallback (for files with syntax issues or truncated reads)
    if re.search(r"DAG\s*\(", content) and "airflow" in content.lower():
        return "airflow_dag"
    if "GlueContext" in content or "awsglue" in content:
        return "glue_job"

    return "unknown"


def build_inventory(source_dir, manifest, verbose=False):
    """Parse all source files and build unified pipeline inventory."""
    inventory = []

    # Parse Airflow DAGs
    fallback_count = 0
    for item in manifest["airflow_dags"]:
        with open(item["full_path"], encoding="utf-8", errors="replace") as f:
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
        with open(item["full_path"], encoding="utf-8", errors="replace") as f:
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
        with open(item["full_path"], encoding="utf-8", errors="replace") as f:
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
        Path(nb_path).write_text(art.notebook_code, encoding="utf-8")

        # Job YAML
        Path(os.path.join(dest, f"resources/jobs/{name}.yml")).write_text(
            art.job_yaml, encoding="utf-8")

        # DQ SQL
        Path(os.path.join(dest, f"quality/{name}_expectations.sql")).write_text(
            art.dq_sql, encoding="utf-8")

        # Governance SQL
        Path(os.path.join(dest, f"governance/column_masks/{name}.sql")).write_text(
            art.governance_sql, encoding="utf-8")

        # Tests
        Path(os.path.join(dest, f"tests/validation/test_{name}.py")).write_text(
            art.test_code, encoding="utf-8")

        # Monitoring
        Path(os.path.join(dest, f"monitoring/{name}_monitoring.sql")).write_text(
            art.monitoring_sql, encoding="utf-8")


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
    Path(os.path.join(dest, "databricks.yml")).write_text(bundle_yml, encoding="utf-8")

    # .gitignore
    gitignore = """.databricks/
.bundle/
__pycache__/
*.pyc
.venv/
.DS_Store
"""
    Path(os.path.join(dest, ".gitignore")).write_text(gitignore, encoding="utf-8")

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
    Path(os.path.join(dest, ".github/workflows/deploy.yml")).write_text(ci_yml, encoding="utf-8")

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

- `src/pipelines/` - Ingestion notebooks (per domain)
- `resources/jobs/` - Job schedules and compute config
- `governance/` - Column masks, row filters, bootstrap SQL
- `quality/` - Data quality expectations
- `monitoring/` - Freshness SLA and volume anomaly queries
- `tests/` - Migration validation tests
"""
    Path(os.path.join(dest, "README.md")).write_text(readme, encoding="utf-8")


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

    # Default mapping — more specific terms first to avoid misclassification
    default_map = {
        "compliance": "compliance", "audit": "compliance", "regulatory": "compliance",
        "lending": "lending", "loan": "lending", "mortgage": "lending",
        "payment": "lending", "invoice": "lending",
        "risk": "risk", "trade": "risk", "market": "risk",
        "customer": "customer", "user": "customer",
        "finance": "finance", "vendor": "finance", "partner": "finance",
        "account": "finance",
    }
    for keyword, domain in default_map.items():
        if keyword in text:
            return domain

    return "analytics"


if __name__ == "__main__":
    main()
