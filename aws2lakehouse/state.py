"""
Migration State — Tracks progress, enables resume, and provides audit trail.

Persists migration state to a JSON file so that large migrations can be
resumed after failure without re-processing already-completed pipelines.
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class StepStatus(str, Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


__all__ = ["MigrationState", "PipelineState", "StepStatus"]


@dataclass
class PipelineState:
    """Tracks the migration state of a single pipeline."""
    name: str
    domain: str
    status: StepStatus = StepStatus.NOT_STARTED
    artifacts_written: list[str] = field(default_factory=list)
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


@dataclass
class MigrationState:
    """Full migration run state — serializable to JSON for resume support."""
    run_id: str
    source: str
    dest: str
    catalog: str
    org: str
    started_at: str = ""
    completed_at: Optional[str] = None
    current_step: str = "scan"
    steps_completed: list[str] = field(default_factory=list)
    pipelines: dict[str, PipelineState] = field(default_factory=dict)
    total_pipelines: int = 0
    completed_pipelines: int = 0
    failed_pipelines: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)

    @property
    def state_file(self) -> str:
        return os.path.join(self.dest, ".migration_state.json")

    def save(self):
        """Persist state to disk."""
        os.makedirs(os.path.dirname(self.state_file) or ".", exist_ok=True)
        data = {
            "run_id": self.run_id,
            "source": self.source,
            "dest": self.dest,
            "catalog": self.catalog,
            "org": self.org,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "current_step": self.current_step,
            "steps_completed": self.steps_completed,
            "total_pipelines": self.total_pipelines,
            "completed_pipelines": self.completed_pipelines,
            "failed_pipelines": self.failed_pipelines,
            "errors": self.errors,
            "pipelines": {
                name: {
                    "name": ps.name,
                    "domain": ps.domain,
                    "status": ps.status.value,
                    "artifacts_written": ps.artifacts_written,
                    "error": ps.error,
                    "started_at": ps.started_at,
                    "completed_at": ps.completed_at,
                }
                for name, ps in self.pipelines.items()
            },
        }
        tmp = self.state_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2, default=str)
        # Atomic rename
        os.replace(tmp, self.state_file)

    @classmethod
    def load(cls, dest: str) -> Optional["MigrationState"]:
        """Load state from a previous run if it exists."""
        state_file = os.path.join(dest, ".migration_state.json")
        if not os.path.exists(state_file):
            return None
        try:
            with open(state_file) as f:
                data = json.load(f)
            state = cls(
                run_id=data["run_id"],
                source=data["source"],
                dest=data["dest"],
                catalog=data["catalog"],
                org=data["org"],
                started_at=data.get("started_at", ""),
                completed_at=data.get("completed_at"),
                current_step=data.get("current_step", "scan"),
                steps_completed=data.get("steps_completed", []),
                total_pipelines=data.get("total_pipelines", 0),
                completed_pipelines=data.get("completed_pipelines", 0),
                failed_pipelines=data.get("failed_pipelines", 0),
                errors=data.get("errors", []),
            )
            for name, ps_data in data.get("pipelines", {}).items():
                state.pipelines[name] = PipelineState(
                    name=ps_data["name"],
                    domain=ps_data["domain"],
                    status=StepStatus(ps_data["status"]),
                    artifacts_written=ps_data.get("artifacts_written", []),
                    error=ps_data.get("error"),
                    started_at=ps_data.get("started_at"),
                    completed_at=ps_data.get("completed_at"),
                )
            return state
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"  WARNING: Could not load state file ({e}), starting fresh.")
            return None

    @classmethod
    def create(cls, source: str, dest: str, catalog: str, org: str) -> "MigrationState":
        """Create a new migration state."""
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        return cls(
            run_id=run_id,
            source=source,
            dest=dest,
            catalog=catalog,
            org=org,
            started_at=datetime.now().isoformat(),
        )

    def mark_step_started(self, step: str):
        self.current_step = step
        self.save()

    def mark_step_completed(self, step: str):
        if step not in self.steps_completed:
            self.steps_completed.append(step)
        self.save()

    def is_step_completed(self, step: str) -> bool:
        return step in self.steps_completed

    def mark_pipeline_started(self, name: str, domain: str):
        if name not in self.pipelines:
            self.pipelines[name] = PipelineState(name=name, domain=domain)
        self.pipelines[name].status = StepStatus.IN_PROGRESS
        self.pipelines[name].started_at = datetime.now().isoformat()
        self.save()

    def mark_pipeline_completed(self, name: str, artifacts: list[str]):
        if name in self.pipelines:
            # Guard against double-increment on resume
            already_done = self.pipelines[name].status == StepStatus.COMPLETED
            self.pipelines[name].status = StepStatus.COMPLETED
            self.pipelines[name].artifacts_written = artifacts
            self.pipelines[name].completed_at = datetime.now().isoformat()
            if not already_done:
                self.completed_pipelines += 1
            self.save()

    def mark_pipeline_failed(self, name: str, error: str):
        if name in self.pipelines:
            self.pipelines[name].status = StepStatus.FAILED
            self.pipelines[name].error = error
            self.failed_pipelines += 1
            self.errors.append({
                "pipeline": name,
                "error": error,
                "timestamp": datetime.now().isoformat(),
            })
            self.save()

    def is_pipeline_completed(self, name: str) -> bool:
        return (name in self.pipelines
                and self.pipelines[name].status == StepStatus.COMPLETED)

    def mark_complete(self):
        self.completed_at = datetime.now().isoformat()
        self.current_step = "done"
        self.save()

    def summary(self) -> str:
        lines = [
            f"Run: {self.run_id}",
            f"Status: {self.current_step}",
            f"Pipelines: {self.completed_pipelines}/{self.total_pipelines} completed",
        ]
        if self.failed_pipelines:
            lines.append(f"Failed: {self.failed_pipelines}")
        if self.errors:
            lines.append("Errors:")
            for err in self.errors[-5:]:  # last 5
                lines.append(f"  - {err['pipeline']}: {err['error'][:80]}")
        return "\n".join(lines)
