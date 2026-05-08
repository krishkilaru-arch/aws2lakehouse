"""Tests for end-to-end migration flow (dry-run and full)."""

import json
import os
import sys
from pathlib import Path


class TestDryRun:
    """Test the --dry-run flag produces specs without writing artifacts."""

    def test_dry_run_completes(self, sample_aws_project, tmp_dir, capsys):
        dest = os.path.join(tmp_dir, "output")
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from migrate import main as migrate_main

        old_argv = sys.argv
        sys.argv = [
            "migrate.py",
            "--source", sample_aws_project,
            "--dest", dest,
            "--dry-run",
        ]
        try:
            migrate_main()
        finally:
            sys.argv = old_argv

        captured = capsys.readouterr()
        assert "DRY RUN COMPLETE" in captured.out
        # Artifacts should NOT be written
        assert not os.path.exists(os.path.join(dest, "databricks.yml"))


class TestFullMigration:
    """Test the full migration produces a complete DAB repo."""

    def test_full_migration(self, sample_aws_project, tmp_dir):
        dest = os.path.join(tmp_dir, "output")
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from migrate import main as migrate_main

        old_argv = sys.argv
        sys.argv = [
            "migrate.py",
            "--source", sample_aws_project,
            "--dest", dest,
            "--catalog", "test_catalog",
            "--org", "testorg",
        ]
        try:
            migrate_main()
        finally:
            sys.argv = old_argv

        # Check output structure
        assert os.path.exists(os.path.join(dest, "databricks.yml"))
        assert os.path.exists(os.path.join(dest, "README.md"))
        assert os.path.isdir(os.path.join(dest, "src", "pipelines"))
        assert os.path.isdir(os.path.join(dest, "resources", "jobs"))
        assert os.path.isdir(os.path.join(dest, "governance"))
        assert os.path.isdir(os.path.join(dest, "quality"))
        assert os.path.isdir(os.path.join(dest, "monitoring"))
        assert os.path.isdir(os.path.join(dest, "tests"))

        # Check state file was created
        assert os.path.exists(os.path.join(dest, ".migration_state.json"))
        with open(os.path.join(dest, ".migration_state.json")) as f:
            state = json.load(f)
        assert state["current_step"] == "done"
        assert state["completed_at"] is not None

        # Check at least one pipeline was generated
        job_files = list(Path(os.path.join(dest, "resources", "jobs")).glob("*.yml"))
        assert len(job_files) >= 1

    def test_resume_migration(self, sample_aws_project, tmp_dir):
        """Running with --resume after a completed migration should be a no-op."""
        dest = os.path.join(tmp_dir, "output")
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from migrate import main as migrate_main

        # First run
        old_argv = sys.argv
        sys.argv = [
            "migrate.py", "--source", sample_aws_project, "--dest", dest,
        ]
        try:
            migrate_main()
        finally:
            sys.argv = old_argv

        # Get state after first run
        with open(os.path.join(dest, ".migration_state.json")) as f:
            json.load(f)

        # Resume run (should skip all steps)
        sys.argv = [
            "migrate.py", "--source", sample_aws_project, "--dest", dest, "--resume",
        ]
        try:
            migrate_main()
        finally:
            sys.argv = old_argv

        # State should still be complete
        with open(os.path.join(dest, ".migration_state.json")) as f:
            state2 = json.load(f)
        assert state2["current_step"] == "done"


class TestCleanMode:
    def test_clean_removes_old_output(self, sample_aws_project, tmp_dir):
        dest = os.path.join(tmp_dir, "output")
        os.makedirs(dest)
        Path(os.path.join(dest, "old_file.txt")).write_text("old data")

        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from migrate import main as migrate_main

        old_argv = sys.argv
        sys.argv = [
            "migrate.py", "--source", sample_aws_project, "--dest", dest, "--clean",
        ]
        try:
            migrate_main()
        finally:
            sys.argv = old_argv

        assert not os.path.exists(os.path.join(dest, "old_file.txt"))
        assert os.path.exists(os.path.join(dest, "databricks.yml"))
