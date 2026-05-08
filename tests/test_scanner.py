"""Tests for the scanner and inventory builder in migrate.py."""

import json
import os

# Import from migrate.py
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from migrate import (
    _detect_python_file_type,
    _extract,
    _extract_domain,
    _is_step_function_json,
    build_inventory,
    scan_source,
)


class TestScanSource:
    """Tests for the file scanner."""

    def test_detects_airflow_dags_by_path(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        dag_files = [e["file"] for e in manifest["airflow_dags"]]
        assert "daily_etl.py" in dag_files

    def test_detects_emr_scripts(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        emr_files = [e["file"] for e in manifest["emr_scripts"]]
        assert "trade_events_streaming.sh" in emr_files

    def test_detects_glue_jobs(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        glue_files = [e["file"] for e in manifest["glue_jobs"]]
        assert "payment_processor.py" in glue_files

    def test_detects_step_functions(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        sf_files = [e["file"] for e in manifest["step_functions"]]
        assert "etl_pipeline.json" in sf_files

    def test_detects_configs(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        assert len(manifest["configs"]) >= 1

    def test_no_duplicates_across_categories(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        all_paths = []
        for category in manifest.values():
            all_paths.extend(e["path"] for e in category)
        assert len(all_paths) == len(set(all_paths)), "Duplicate file across categories"

    def test_content_based_dag_detection(self, tmp_dir):
        """DAG in a non-standard path should still be detected by AST."""
        scripts_dir = os.path.join(tmp_dir, "scripts")
        os.makedirs(scripts_dir)
        Path(os.path.join(scripts_dir, "my_pipeline.py")).write_text('''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG("my_dag", schedule_interval="@daily", start_date=datetime(2024,1,1)):
    t = PythonOperator(task_id="run", python_callable=lambda: None)
''')
        manifest = scan_source(tmp_dir)
        dag_files = [e["file"] for e in manifest["airflow_dags"]]
        assert "my_pipeline.py" in dag_files

    def test_content_based_glue_detection(self, tmp_dir):
        """Glue job in a non-standard path should still be detected."""
        scripts_dir = os.path.join(tmp_dir, "etl")
        os.makedirs(scripts_dir)
        Path(os.path.join(scripts_dir, "etl_job.py")).write_text('''
from awsglue.context import GlueContext
from pyspark.context import SparkContext
gc = GlueContext(SparkContext())
''')
        manifest = scan_source(tmp_dir)
        glue_files = [e["file"] for e in manifest["glue_jobs"]]
        assert "etl_job.py" in glue_files

    def test_skips_git_directory(self, tmp_dir):
        """Files in .git should be ignored."""
        git_dir = os.path.join(tmp_dir, ".git", "objects")
        os.makedirs(git_dir)
        Path(os.path.join(git_dir, "something.py")).write_text("x = 1")
        manifest = scan_source(tmp_dir)
        all_files = []
        for cat in manifest.values():
            all_files.extend(e["file"] for e in cat)
        assert "something.py" not in all_files

    def test_empty_directory(self, tmp_dir):
        manifest = scan_source(tmp_dir)
        total = sum(len(v) for v in manifest.values())
        assert total == 0


class TestStepFunctionDetection:
    def test_valid_step_function(self, tmp_dir):
        sf = os.path.join(tmp_dir, "sf.json")
        Path(sf).write_text(json.dumps({
            "StartAt": "Step1",
            "States": {"Step1": {"Type": "Task", "End": True}}
        }))
        assert _is_step_function_json(sf) is True

    def test_non_step_function_json(self, tmp_dir):
        other = os.path.join(tmp_dir, "other.json")
        Path(other).write_text(json.dumps({"key": "value"}))
        assert _is_step_function_json(other) is False


class TestPythonFileDetection:
    def test_airflow_detection(self, tmp_dir):
        f = os.path.join(tmp_dir, "dag.py")
        Path(f).write_text('''
from airflow import DAG
with DAG("test", schedule_interval="@daily"):
    pass
''')
        assert _detect_python_file_type(f) == "airflow_dag"

    def test_glue_detection(self, tmp_dir):
        f = os.path.join(tmp_dir, "glue.py")
        Path(f).write_text('''
from awsglue.context import GlueContext
gc = GlueContext(sc)
''')
        assert _detect_python_file_type(f) == "glue_job"

    def test_plain_python(self, tmp_dir):
        f = os.path.join(tmp_dir, "util.py")
        Path(f).write_text("def helper(): return 42")
        assert _detect_python_file_type(f) == "unknown"


class TestBuildInventory:
    def test_builds_from_sample_project(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        inventory = build_inventory(sample_aws_project, manifest)
        assert len(inventory) >= 3  # at least DAG + EMR + Glue
        names = [p["name"] for p in inventory]
        assert "daily_customer_etl" in names
        assert "trade_events_streaming" in names
        assert "payment_processor" in names

    def test_dag_id_extracted_correctly(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        inventory = build_inventory(sample_aws_project, manifest)
        dag = next(p for p in inventory if p["name"] == "daily_customer_etl")
        assert dag["schedule"] == "0 6 * * *"
        assert dag["classification"] == "confidential"  # has "pii" tag

    def test_emr_streaming_detected(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        inventory = build_inventory(sample_aws_project, manifest)
        emr = next(p for p in inventory if p["name"] == "trade_events_streaming")
        assert emr["source_type"] == "kafka"
        assert emr["classification"] == "mnpi"

    def test_glue_jdbc_detected(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        inventory = build_inventory(sample_aws_project, manifest)
        glue = next(p for p in inventory if p["name"] == "payment_processor")
        assert glue["source_type"] == "jdbc"

    def test_all_entries_have_required_keys(self, sample_aws_project):
        manifest = scan_source(sample_aws_project)
        inventory = build_inventory(sample_aws_project, manifest)
        required_keys = {"name", "domain", "source_type", "schedule",
                         "sla_minutes", "business_impact", "owner", "classification"}
        for p in inventory:
            missing = required_keys - set(p.keys())
            assert not missing, f"Pipeline {p['name']} missing keys: {missing}"


class TestExtractHelpers:
    def test_extract_pattern_match(self):
        assert _extract(r'name="(\w+)"', 'name="test"') == "test"

    def test_extract_no_match(self):
        assert _extract(r'name="(\w+)"', "no match here") is None

    def test_extract_domain_risk(self):
        assert _extract_domain("", "trade_events") == "risk"

    def test_extract_domain_lending(self):
        assert _extract_domain("", "loan_processor") == "lending"

    def test_extract_domain_default(self):
        assert _extract_domain("", "misc_task") == "analytics"

    def test_extract_domain_custom_mapping(self):
        custom = {"treasury": "treasury"}
        assert _extract_domain("", "treasury_report", domain_mapping=custom) == "treasury"

    def test_extract_domain_from_tags(self):
        assert _extract_domain("compliance audit", "job1") == "compliance"
