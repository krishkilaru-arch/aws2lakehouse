"""Tests for migration state management."""

import json
import os

from aws2lakehouse.state import MigrationState, StepStatus


class TestMigrationState:
    def test_create(self, tmp_dir):
        state = MigrationState.create(
            source="/src", dest=tmp_dir, catalog="prod", org="acme"
        )
        assert state.run_id  # non-empty
        assert state.source == "/src"
        assert state.dest == tmp_dir
        assert state.current_step == "scan"
        assert state.completed_pipelines == 0

    def test_save_and_load(self, tmp_dir):
        state = MigrationState.create("/src", tmp_dir, "prod", "acme")
        state.mark_step_completed("scan")
        state.mark_pipeline_started("pipe1", "risk")
        state.mark_pipeline_completed("pipe1", ["a.py", "b.yml"])
        state.save()

        loaded = MigrationState.load(tmp_dir)
        assert loaded is not None
        assert loaded.run_id == state.run_id
        assert loaded.is_step_completed("scan")
        assert loaded.is_pipeline_completed("pipe1")
        assert loaded.pipelines["pipe1"].artifacts_written == ["a.py", "b.yml"]

    def test_load_nonexistent(self, tmp_dir):
        assert MigrationState.load(tmp_dir) is None

    def test_load_corrupt_file(self, tmp_dir):
        os.makedirs(tmp_dir, exist_ok=True)
        with open(os.path.join(tmp_dir, ".migration_state.json"), "w") as f:
            f.write("not valid json{{{")
        assert MigrationState.load(tmp_dir) is None

    def test_step_tracking(self, tmp_dir):
        state = MigrationState.create("/src", tmp_dir, "prod", "acme")
        assert not state.is_step_completed("scan")
        state.mark_step_started("scan")
        assert state.current_step == "scan"
        state.mark_step_completed("scan")
        assert state.is_step_completed("scan")
        assert not state.is_step_completed("parse")

    def test_pipeline_lifecycle(self, tmp_dir):
        state = MigrationState.create("/src", tmp_dir, "prod", "acme")
        state.total_pipelines = 2

        # Pipeline 1: success
        state.mark_pipeline_started("p1", "risk")
        assert state.pipelines["p1"].status == StepStatus.IN_PROGRESS
        state.mark_pipeline_completed("p1", ["notebook.py"])
        assert state.pipelines["p1"].status == StepStatus.COMPLETED
        assert state.completed_pipelines == 1

        # Pipeline 2: failure
        state.mark_pipeline_started("p2", "finance")
        state.mark_pipeline_failed("p2", "ValueError: bad data")
        assert state.pipelines["p2"].status == StepStatus.FAILED
        assert state.failed_pipelines == 1
        assert len(state.errors) == 1

    def test_resume_skips_completed(self, tmp_dir):
        state = MigrationState.create("/src", tmp_dir, "prod", "acme")
        state.mark_pipeline_started("done_pipe", "risk")
        state.mark_pipeline_completed("done_pipe", ["a.py"])
        assert state.is_pipeline_completed("done_pipe")
        assert not state.is_pipeline_completed("new_pipe")

    def test_mark_complete(self, tmp_dir):
        state = MigrationState.create("/src", tmp_dir, "prod", "acme")
        state.mark_complete()
        assert state.completed_at is not None
        assert state.current_step == "done"

    def test_summary(self, tmp_dir):
        state = MigrationState.create("/src", tmp_dir, "prod", "acme")
        state.total_pipelines = 5
        state.completed_pipelines = 3
        state.failed_pipelines = 1
        state.errors.append({"pipeline": "bad", "error": "boom", "timestamp": "now"})
        summary = state.summary()
        assert "3/5" in summary
        assert "bad" in summary

    def test_atomic_save(self, tmp_dir):
        """State file should not be corrupt if process dies mid-write."""
        state = MigrationState.create("/src", tmp_dir, "prod", "acme")
        state.save()
        # File should exist and be valid JSON
        state_file = os.path.join(tmp_dir, ".migration_state.json")
        assert os.path.exists(state_file)
        with open(state_file) as f:
            data = json.load(f)
        assert data["run_id"] == state.run_id
        # Tmp file should be cleaned up
        assert not os.path.exists(state_file + ".tmp")
