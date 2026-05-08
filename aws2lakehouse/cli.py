#!/usr/bin/env python3
"""
aws2lakehouse CLI — Unified command-line interface.

Entry point for the `aws2lakehouse` console script installed via pip.
Delegates to sub-commands for migration, scanning, validation, and utilities.

Usage:
    aws2lakehouse migrate --source ./aws-repo --dest ./dbx-repo
    aws2lakehouse scan --source ./aws-repo
    aws2lakehouse validate --source ./aws-repo --dest ./dbx-repo
    aws2lakehouse roi --config roi_inputs.yaml
    aws2lakehouse compare --source-file job.py --target-file notebook.py
"""

import argparse
import json
import os
import sys
from pathlib import Path


def _import_migrate():
    """Import migrate module - handles both installed and dev layouts."""
    try:
        import migrate
        return migrate
    except ImportError:
        accelerator_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if accelerator_dir not in sys.path:
            sys.path.insert(0, accelerator_dir)
        import migrate
        return migrate


def cmd_migrate(args):
    """Run the full migration pipeline (delegates to migrate.py main)."""
    # Build sys.argv to match what migrate.py expects
    migrate_args = ["migrate.py", "--source", args.source, "--dest", args.dest]
    if args.catalog:
        migrate_args += ["--catalog", args.catalog]
    if args.org:
        migrate_args += ["--org", args.org]
    if args.clean:
        migrate_args.append("--clean")
    if args.ai:
        migrate_args.append("--ai")
    if args.ai_model:
        migrate_args += ["--ai-model", args.ai_model]
    if args.dry_run:
        migrate_args.append("--dry-run")
    if args.threads and args.threads > 1:
        migrate_args += ["--threads", str(args.threads)]
    if args.verbose:
        migrate_args.append("--verbose")
    if args.resume:
        migrate_args.append("--resume")

    # Import and invoke migrate.main() directly
    migrate = _import_migrate()

    old_argv = sys.argv
    sys.argv = migrate_args
    try:
        migrate.main()
    finally:
        sys.argv = old_argv


def cmd_scan(args):
    """Scan a source repository and print inventory summary."""
    migrate = _import_migrate()

    source = os.path.abspath(args.source)
    if not os.path.isdir(source):
        print(f"ERROR: Source directory not found: {source}", file=sys.stderr)
        sys.exit(1)

    manifest = migrate.scan_source(source)
    total = sum(len(v) for v in manifest.values())
    print(f"Scanned: {total} files")
    for category, items in manifest.items():
        if items:
            print(f"  {category}: {len(items)}")
            if args.verbose:
                for item in items:
                    print(f"    - {item['path']}")

    if args.inventory:
        inventory = migrate.build_inventory(source, manifest, verbose=args.verbose)
        print(f"\nInventory: {len(inventory)} pipelines")
        for p in inventory:
            print(f"  [{p['domain']}] {p['name']} ({p['source_type']}) "
                  f"classification={p.get('classification', 'internal')}")

    if args.output:
        inventory = migrate.build_inventory(source, manifest, verbose=False)
        output_path = Path(args.output)
        output_path.write_text(
            json.dumps(inventory, indent=2, default=str), encoding="utf-8"
        )
        print(f"\nInventory written to {args.output}")


def cmd_validate(args):
    """Run post-migration validation checks."""
    from aws2lakehouse.validation import MigrationValidator

    MigrationValidator()
    print(f"Validation target: {args.dest}")
    print("Checking artifact completeness...")

    dest = Path(args.dest)
    checks = {
        "databricks.yml": dest / "databricks.yml",
        "src/pipelines/": dest / "src" / "pipelines",
        "resources/jobs/": dest / "resources" / "jobs",
        "governance/": dest / "governance",
        "quality/": dest / "quality",
        "monitoring/": dest / "monitoring",
        "tests/": dest / "tests",
    }

    passed = 0
    failed = 0
    for name, path in checks.items():
        exists = path.exists()
        status = "PASS" if exists else "FAIL"
        icon = "+" if exists else "X"
        print(f"  [{icon}] {name}: {status}")
        if exists:
            passed += 1
        else:
            failed += 1

    # Count artifacts
    if (dest / "resources" / "jobs").exists():
        job_count = len(list((dest / "resources" / "jobs").glob("*.yml")))
        print(f"\n  Job definitions: {job_count}")

    if (dest / "src" / "pipelines").exists():
        notebook_count = len(list((dest / "src" / "pipelines").rglob("*.py")))
        print(f"  Notebooks: {notebook_count}")

    print(f"\nResult: {passed} passed, {failed} failed")
    if failed > 0:
        sys.exit(1)


def cmd_roi(args):
    """Calculate migration ROI from a config file or interactive prompts."""
    from aws2lakehouse.roi_calculator import AWSCurrentState, DatabricksProjectedState, ROICalculator

    if args.config:
        import yaml
        with open(args.config) as f:
            data = yaml.safe_load(f)
        aws_state = AWSCurrentState(**data.get("aws_current", {}))
        dbx_state = DatabricksProjectedState(**data.get("databricks_projected", {}))
        migration_cost = data.get("migration_cost", 0.0)
    else:
        # Minimal interactive mode with defaults
        aws_state = AWSCurrentState()
        dbx_state = DatabricksProjectedState()
        migration_cost = 0.0
        print("Using default cost estimates. Pass --config roi.yaml for custom values.")

    calc = ROICalculator(
        current=aws_state,
        projected=dbx_state,
        migration_cost=migration_cost,
    )
    result = calc.calculate()
    summary = calc.generate_executive_summary()
    print(summary)

    if args.output:
        Path(args.output).write_text(
            json.dumps(result, indent=2, default=str), encoding="utf-8"
        )
        print(f"\nROI data written to {args.output}")


def cmd_compare(args):
    """Run AI-powered semantic comparison between source and target code."""
    from aws2lakehouse.genai.ai_client import AIClient

    source_code = Path(args.source_file).read_text()
    target_code = Path(args.target_file).read_text()

    ai = AIClient(model=args.model)
    result = ai.compare_migration(
        source_code=source_code,
        target_code=target_code,
        pipeline_name=args.name or Path(args.source_file).stem,
        source_type=args.source_type,
    )
    print(result)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="aws2lakehouse",
        description="AWS2Lakehouse — Enterprise AWS to Databricks Migration Accelerator",
    )
    from aws2lakehouse import __version__
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ── migrate ──
    migrate_p = subparsers.add_parser("migrate", help="Run full migration pipeline")
    migrate_p.add_argument("--source", required=True, help="Path to source AWS project")
    migrate_p.add_argument("--dest", required=True, help="Path for output Databricks project")
    migrate_p.add_argument("--catalog", default="production", help="Target Unity Catalog name")
    migrate_p.add_argument("--org", default="acme", help="Organization name")
    migrate_p.add_argument("--clean", action="store_true", help="Delete destination before generating")
    migrate_p.add_argument("--ai", action="store_true", help="Use AI for enhanced code transformation")
    migrate_p.add_argument("--ai-model", default="opus", choices=["opus", "sonnet", "haiku"])
    migrate_p.add_argument("--dry-run", action="store_true", help="Preview without writing artifacts")
    migrate_p.add_argument("--threads", type=int, default=1, help="Parallel threads for AI calls")
    migrate_p.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    migrate_p.add_argument("--verbose", "-v", action="store_true")
    migrate_p.set_defaults(func=cmd_migrate)

    # ── scan ──
    scan_p = subparsers.add_parser("scan", help="Scan source repo and report inventory")
    scan_p.add_argument("--source", required=True, help="Path to source AWS project")
    scan_p.add_argument("--inventory", action="store_true", help="Build full pipeline inventory")
    scan_p.add_argument("--output", "-o", help="Write inventory JSON to file")
    scan_p.add_argument("--verbose", "-v", action="store_true")
    scan_p.set_defaults(func=cmd_scan)

    # ── validate ──
    validate_p = subparsers.add_parser("validate", help="Validate migration output")
    validate_p.add_argument("--dest", required=True, help="Path to Databricks output project")
    validate_p.set_defaults(func=cmd_validate)

    # ── roi ──
    roi_p = subparsers.add_parser("roi", help="Calculate migration ROI")
    roi_p.add_argument("--config", help="YAML file with AWS cost inputs")
    roi_p.add_argument("--output", "-o", help="Write ROI data to JSON file")
    roi_p.set_defaults(func=cmd_roi)

    # ── compare ──
    compare_p = subparsers.add_parser("compare", help="AI semantic comparison of source vs target code")
    compare_p.add_argument("--source-file", required=True, help="Original AWS source code file")
    compare_p.add_argument("--target-file", required=True, help="Generated Databricks code file")
    compare_p.add_argument("--name", help="Pipeline name for the report")
    compare_p.add_argument("--source-type", default="emr", choices=["emr", "glue", "airflow", "step_function"])
    compare_p.add_argument("--model", default="opus", choices=["opus", "sonnet", "haiku"])
    compare_p.set_defaults(func=cmd_compare)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
