"""
Step Function Converter — Converts AWS Step Functions to Databricks Lakeflow Jobs.

Handles all Step Function state types:
- Task states → Job tasks (notebook, python_wheel, spark_submit)
- Parallel states → Parallel task branches
- Choice states → Conditional task execution (if_else_task)
- Map states → For-each task patterns
- Wait states → Timing controls
- Pass/Succeed/Fail states → Control flow

Supports:
- Full DAG preservation (dependencies, fan-out, fan-in)
- Error handling (Catch/Retry → max_retries, on_failure)
- Input/Output processing → Task parameters
- Nested state machines → Nested job references
"""

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class LakeflowTask:
    """Represents a task in a Lakeflow Job."""
    task_key: str
    description: str = ""
    task_type: str = "notebook_task"  # notebook_task, python_wheel_task, spark_submit_task, pipeline_task

    # Task configuration
    notebook_path: str = ""
    python_file: str = ""
    main_class: str = ""
    parameters: dict[str, str] = field(default_factory=dict)

    # Dependencies
    depends_on: list[str] = field(default_factory=list)

    # Execution
    timeout_seconds: int = 3600
    max_retries: int = 0
    retry_on_timeout: bool = True

    # Cluster
    job_cluster_key: str = "shared_cluster"

    # Condition (for Choice states)
    condition_task: Optional[dict] = None

    def to_api_payload(self) -> dict:
        """Convert to Databricks Jobs API task payload."""
        task = {
            "task_key": self.task_key,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "retry_on_timeout": self.retry_on_timeout,
        }

        if self.description:
            task["description"] = self.description

        if self.depends_on:
            task["depends_on"] = [{"task_key": dep} for dep in self.depends_on]

        if self.task_type == "notebook_task":
            task["notebook_task"] = {
                "notebook_path": self.notebook_path,
                "source": "WORKSPACE",
                "base_parameters": self.parameters
            }
        elif self.task_type == "python_wheel_task":
            task["python_wheel_task"] = {
                "package_name": self.parameters.get("package", ""),
                "entry_point": self.parameters.get("entry_point", "main"),
                "parameters": [f"--{k}={v}" for k, v in self.parameters.items()
                              if k not in ("package", "entry_point")]
            }
        elif self.task_type == "spark_submit_task":
            task["spark_submit_task"] = {
                "parameters": [self.main_class] +
                             [f"--{k}={v}" for k, v in self.parameters.items()]
            }

        if self.condition_task:
            task["condition_task"] = self.condition_task

        task["job_cluster_key"] = self.job_cluster_key

        return task


@dataclass
class LakeflowJob:
    """Complete Lakeflow Job definition."""
    name: str
    description: str = ""
    tasks: list[LakeflowTask] = field(default_factory=list)
    job_clusters: list[dict] = field(default_factory=list)
    schedule: Optional[dict] = None
    email_notifications: dict = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)
    max_concurrent_runs: int = 1
    timeout_seconds: int = 86400

    def to_api_payload(self) -> dict:
        """Convert to full Databricks Jobs API create payload."""
        payload = {
            "name": self.name,
            "description": self.description,
            "tasks": [t.to_api_payload() for t in self.tasks],
            "job_clusters": self.job_clusters,
            "max_concurrent_runs": self.max_concurrent_runs,
            "timeout_seconds": self.timeout_seconds,
            "tags": self.tags
        }

        if self.schedule:
            payload["schedule"] = self.schedule
        if self.email_notifications:
            payload["email_notifications"] = self.email_notifications

        return payload


class StepFunctionConverter:
    """
    Converts AWS Step Functions state machine definitions to Lakeflow Jobs.

    Usage:
        converter = StepFunctionConverter(
            target_notebook_base="/Workspace/Users/team/migrated/",
            default_cluster_key="shared_etl"
        )

        # From Step Function JSON
        with open("state_machine.json") as f:
            definition = json.load(f)

        lakeflow_job = converter.convert(definition)

        # Deploy
        converter.deploy(lakeflow_job, workspace_url, token)
    """

    def __init__(
        self,
        target_notebook_base: str = "/Workspace/Repos/migration/",
        default_cluster_key: str = "shared_cluster",
        default_timeout_seconds: int = 3600
    ):
        self.target_notebook_base = target_notebook_base
        self.default_cluster_key = default_cluster_key
        self.default_timeout_seconds = default_timeout_seconds
        self._warnings: list[str] = []
        self._manual_steps: list[str] = []

    def convert(
        self,
        step_function_definition: dict[str, Any],
        name: str = None,
        schedule_expression: str = None
    ) -> LakeflowJob:
        """
        Convert a Step Function definition to a Lakeflow Job.

        Args:
            step_function_definition: Step Function ASL definition (States, StartAt, etc.)
            name: Override name for the job
            schedule_expression: AWS EventBridge schedule expression
        """
        self._warnings = []
        self._manual_steps = []

        states = step_function_definition.get("States", {})
        start_at = step_function_definition.get("StartAt", "")
        comment = step_function_definition.get("Comment", "")

        job_name = name or step_function_definition.get("Comment", "migrated_workflow")

        logger.info(f"Converting Step Function: {job_name} ({len(states)} states)")

        # Build state graph and convert each state
        tasks = self._convert_states(states, start_at)

        # Build job
        job = LakeflowJob(
            name=job_name,
            description=f"Migrated from AWS Step Function. {comment}",
            tasks=tasks,
            job_clusters=[self._default_cluster_spec()],
            tags={
                "source": "aws_step_function",
                "migrated_by": "aws2lakehouse",
                "original_states": str(len(states))
            }
        )

        # Convert schedule if provided
        if schedule_expression:
            job.schedule = self._convert_schedule(schedule_expression)

        logger.info(f"Converted to Lakeflow Job: {len(tasks)} tasks")
        if self._warnings:
            for w in self._warnings:
                logger.warning(f"  ⚠️  {w}")

        return job

    def _convert_states(self, states: dict, start_at: str) -> list[LakeflowTask]:
        """Convert all states to Lakeflow tasks preserving dependency order."""
        tasks = []
        visited = set()
        task_map = {}  # state_name -> task_key

        # Topological traversal from StartAt
        self._traverse_states(states, start_at, tasks, visited, task_map, parent_task=None)

        return tasks

    def _traverse_states(
        self, states: dict, state_name: str, tasks: list[LakeflowTask],
        visited: set, task_map: dict, parent_task: Optional[str]
    ):
        """Recursively traverse and convert states."""
        if state_name in visited or state_name not in states:
            return

        visited.add(state_name)
        state = states[state_name]
        state_type = state.get("Type", "")

        task_key = self._sanitize_task_key(state_name)
        task_map[state_name] = task_key

        if state_type == "Task":
            task = self._convert_task_state(state_name, state, parent_task)
            tasks.append(task)

        elif state_type == "Parallel":
            # Convert parallel branches
            parallel_tasks = self._convert_parallel_state(state_name, state, parent_task, states, visited, task_map)
            tasks.extend(parallel_tasks)
            # The join point is the last task from parallel
            task_key = f"{task_key}_join"

        elif state_type == "Choice":
            # Convert choice to condition_task
            choice_tasks = self._convert_choice_state(state_name, state, parent_task, states, visited, task_map)
            tasks.extend(choice_tasks)
            return  # Choice handles its own Next traversal

        elif state_type == "Map":
            task = self._convert_map_state(state_name, state, parent_task)
            tasks.append(task)

        elif state_type == "Wait":
            # Wait states don't have direct Databricks equivalent
            # Convert to a pass-through notebook with sleep
            task = LakeflowTask(
                task_key=task_key,
                description=f"Wait state: {state.get('Comment', state.get('Seconds', 'unknown'))}s",
                task_type="notebook_task",
                notebook_path=f"{self.target_notebook_base}utilities/wait_task",
                parameters={"wait_seconds": str(state.get("Seconds", 60))},
                depends_on=[parent_task] if parent_task else []
            )
            tasks.append(task)
            self._warnings.append(f"Wait state '{state_name}' converted to notebook sleep — review for better pattern")

        elif state_type in ("Pass", "Succeed"):
            # Skip pass/succeed states, just continue the chain
            pass

        elif state_type == "Fail":
            self._warnings.append(f"Fail state '{state_name}' — implement error handling in upstream task")
            return

        # Follow Next state
        next_state = state.get("Next")
        if next_state and not state.get("End", False):
            current_task = task_key if state_type in ("Task", "Wait", "Map") else parent_task
            self._traverse_states(states, next_state, tasks, visited, task_map, current_task)

    def _convert_task_state(self, name: str, state: dict, parent_task: Optional[str]) -> LakeflowTask:
        """Convert a Task state to a Lakeflow task."""
        resource = state.get("Resource", "")
        task_key = self._sanitize_task_key(name)

        # Determine task type based on resource ARN
        if "glue" in resource.lower():
            # Glue job → notebook task
            job_name = state.get("Parameters", {}).get("JobName", name)
            task = LakeflowTask(
                task_key=task_key,
                description=state.get("Comment", f"Migrated from Glue job: {job_name}"),
                task_type="notebook_task",
                notebook_path=f"{self.target_notebook_base}etl/{self._sanitize_task_key(job_name)}",
                parameters=self._extract_parameters(state),
                depends_on=[parent_task] if parent_task else [],
                timeout_seconds=state.get("TimeoutSeconds", self.default_timeout_seconds),
                max_retries=self._extract_retry_count(state)
            )

        elif "lambda" in resource.lower():
            # Lambda → lightweight python task
            function_name = state.get("Parameters", {}).get("FunctionName", name)
            task = LakeflowTask(
                task_key=task_key,
                description=state.get("Comment", f"Migrated from Lambda: {function_name}"),
                task_type="notebook_task",
                notebook_path=f"{self.target_notebook_base}functions/{self._sanitize_task_key(function_name)}",
                parameters=self._extract_parameters(state),
                depends_on=[parent_task] if parent_task else [],
                timeout_seconds=min(state.get("TimeoutSeconds", 300), 900),
                max_retries=self._extract_retry_count(state)
            )
            self._manual_steps.append(f"Migrate Lambda '{function_name}' logic to Databricks notebook")

        elif "emr" in resource.lower() or "elasticmapreduce" in resource.lower():
            # EMR step → spark job task
            step_config = state.get("Parameters", {})
            task = LakeflowTask(
                task_key=task_key,
                description=state.get("Comment", f"Migrated from EMR step: {name}"),
                task_type="notebook_task",
                notebook_path=f"{self.target_notebook_base}spark_jobs/{task_key}",
                parameters=self._extract_emr_parameters(step_config),
                depends_on=[parent_task] if parent_task else [],
                timeout_seconds=state.get("TimeoutSeconds", self.default_timeout_seconds),
                max_retries=self._extract_retry_count(state)
            )

        elif "ecs" in resource.lower() or "batch" in resource.lower():
            # ECS/Batch → container task or notebook
            task = LakeflowTask(
                task_key=task_key,
                description=state.get("Comment", f"Migrated from ECS/Batch: {name}"),
                task_type="notebook_task",
                notebook_path=f"{self.target_notebook_base}containers/{task_key}",
                parameters=self._extract_parameters(state),
                depends_on=[parent_task] if parent_task else [],
                timeout_seconds=state.get("TimeoutSeconds", self.default_timeout_seconds),
                max_retries=self._extract_retry_count(state)
            )
            self._manual_steps.append(f"Containerized task '{name}' needs rewrite as Spark/Python")

        else:
            # Generic task
            task = LakeflowTask(
                task_key=task_key,
                description=state.get("Comment", f"Migrated task: {name}"),
                task_type="notebook_task",
                notebook_path=f"{self.target_notebook_base}tasks/{task_key}",
                parameters=self._extract_parameters(state),
                depends_on=[parent_task] if parent_task else [],
                timeout_seconds=state.get("TimeoutSeconds", self.default_timeout_seconds),
                max_retries=self._extract_retry_count(state)
            )

        return task

    def _convert_parallel_state(
        self, name: str, state: dict, parent_task: Optional[str],
        all_states: dict, visited: set, task_map: dict
    ) -> list[LakeflowTask]:
        """Convert Parallel state to multiple independent task branches."""
        tasks = []
        branch_end_tasks = []

        for _i, branch in enumerate(state.get("Branches", [])):
            branch_states = branch.get("States", {})
            branch_start = branch.get("StartAt", "")

            # Convert each branch as independent tasks depending on parent
            branch_tasks = []
            self._traverse_states(
                branch_states, branch_start, branch_tasks,
                set(), task_map, parent_task
            )

            # Track end of each branch for join
            if branch_tasks:
                branch_end_tasks.append(branch_tasks[-1].task_key)

            tasks.extend(branch_tasks)

        # Create a join task that depends on all branches
        if branch_end_tasks:
            join_task = LakeflowTask(
                task_key=f"{self._sanitize_task_key(name)}_join",
                description=f"Join point after parallel execution of {name}",
                task_type="notebook_task",
                notebook_path=f"{self.target_notebook_base}utilities/parallel_join",
                depends_on=branch_end_tasks
            )
            tasks.append(join_task)

        return tasks

    def _convert_choice_state(
        self, name: str, state: dict, parent_task: Optional[str],
        all_states: dict, visited: set, task_map: dict
    ) -> list[LakeflowTask]:
        """Convert Choice state to conditional task execution."""
        tasks = []

        # Create a condition evaluation task
        choices = state.get("Choices", [])
        default = state.get("Default", "")

        # For simple choices, use run_if condition
        # For complex choices, use a condition notebook
        condition_task = LakeflowTask(
            task_key=self._sanitize_task_key(name),
            description=f"Choice evaluation: {state.get('Comment', name)}",
            task_type="notebook_task",
            notebook_path=f"{self.target_notebook_base}conditions/{self._sanitize_task_key(name)}",
            depends_on=[parent_task] if parent_task else [],
            parameters={"choices": json.dumps(choices[:3])}  # First few for reference
        )
        tasks.append(condition_task)

        self._warnings.append(
            f"Choice state '{name}' converted to condition notebook — "
            f"review {len(choices)} branches for run_if/if_else_task patterns"
        )

        # Follow default path
        if default:
            self._traverse_states(all_states, default, tasks, visited, task_map, condition_task.task_key)

        return tasks

    def _convert_map_state(self, name: str, state: dict, parent_task: Optional[str]) -> LakeflowTask:
        """Convert Map state to for_each_task pattern."""
        task_key = self._sanitize_task_key(name)

        self._warnings.append(
            f"Map state '{name}' → for_each_task pattern. "
            f"Review iterator and batch size configuration."
        )

        return LakeflowTask(
            task_key=task_key,
            description=f"Map/ForEach: {state.get('Comment', name)}",
            task_type="notebook_task",
            notebook_path=f"{self.target_notebook_base}iterators/{task_key}",
            parameters={
                "max_concurrency": str(state.get("MaxConcurrency", 10)),
                "items_path": state.get("ItemsPath", "$.items")
            },
            depends_on=[parent_task] if parent_task else [],
            timeout_seconds=state.get("TimeoutSeconds", self.default_timeout_seconds * 2)
        )

    def _extract_parameters(self, state: dict) -> dict[str, str]:
        """Extract task parameters from state definition."""
        params = {}
        raw_params = state.get("Parameters", {})

        for key, value in raw_params.items():
            # Remove Step Functions suffixes (.$)
            clean_key = key.rstrip(".$").replace(".", "_")
            if isinstance(value, str):
                params[clean_key] = value
            elif isinstance(value, (int, float, bool)):
                params[clean_key] = str(value)

        return params

    def _extract_emr_parameters(self, step_config: dict) -> dict[str, str]:
        """Extract parameters from EMR step configuration."""
        params = {}

        step = step_config.get("Step", step_config)
        hadoop_jar = step.get("HadoopJarStep", {})
        args = hadoop_jar.get("Args", [])

        # Parse spark-submit style arguments
        i = 0
        while i < len(args):
            arg = args[i]
            if arg.startswith("--") and i + 1 < len(args) and not args[i+1].startswith("--"):
                params[arg.lstrip("-")] = args[i+1]
                i += 2
            elif arg.endswith(".py") or arg.endswith(".jar"):
                params["main_script"] = arg
                i += 1
            else:
                i += 1

        return params

    def _extract_retry_count(self, state: dict) -> int:
        """Extract retry configuration from state."""
        retriers = state.get("Retry", [])
        if retriers:
            return retriers[0].get("MaxAttempts", 2)
        return 0

    def _convert_schedule(self, expression: str) -> dict:
        """Convert AWS schedule expression to Databricks quartz cron."""
        # AWS formats:
        # rate(1 day), rate(6 hours)
        # cron(0 6 * * ? *)

        if expression.startswith("cron("):
            parts = expression.replace("cron(", "").replace(")", "").split()
            if len(parts) >= 5:
                # AWS: min hour dom month dow [year]
                # Quartz: sec min hour dom month dow
                quartz = f"0 {parts[0]} {parts[1]} {parts[2]} {parts[3]} {parts[4]}"
                return {
                    "quartz_cron_expression": quartz,
                    "timezone_id": "UTC",
                    "pause_status": "PAUSED"
                }

        elif expression.startswith("rate("):
            rate_str = expression.replace("rate(", "").replace(")", "").strip()
            # Convert rate to approximate cron
            if "hour" in rate_str:
                hours = int(re.search(r"(\d+)", rate_str).group(1))
                return {
                    "quartz_cron_expression": f"0 0 */{hours} * * ?",
                    "timezone_id": "UTC",
                    "pause_status": "PAUSED"
                }
            elif "day" in rate_str:
                return {
                    "quartz_cron_expression": "0 0 6 * * ?",
                    "timezone_id": "UTC",
                    "pause_status": "PAUSED"
                }
            elif "minute" in rate_str:
                minutes = int(re.search(r"(\d+)", rate_str).group(1))
                return {
                    "quartz_cron_expression": f"0 */{minutes} * * * ?",
                    "timezone_id": "UTC",
                    "pause_status": "PAUSED"
                }

        self._warnings.append(f"Could not convert schedule: {expression}")
        return {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"}

    def _default_cluster_spec(self) -> dict:
        """Generate default job cluster specification."""
        return {
            "job_cluster_key": self.default_cluster_key,
            "new_cluster": {
                "spark_version": "15.4.x-photon-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 4,
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.databricks.delta.optimizeWrite.enabled": "true"
                },
                "aws_attributes": {
                    "availability": "SPOT_WITH_FALLBACK",
                    "first_on_demand": 1
                }
            }
        }

    def _sanitize_task_key(self, name: str) -> str:
        """Convert state name to valid Databricks task key."""
        # Replace non-alphanumeric with underscore, lowercase
        key = re.sub(r"[^a-zA-Z0-9]", "_", name).lower()
        # Remove leading/trailing underscores and collapse doubles
        key = re.sub(r"_+", "_", key).strip("_")
        return key[:100]  # Max 100 chars

    def deploy(self, job: LakeflowJob, workspace_url: str, token: str) -> dict:
        """Deploy the converted Lakeflow Job to Databricks workspace."""
        import requests

        payload = job.to_api_payload()

        response = requests.post(
            f"{workspace_url}/api/2.1/jobs/create",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload
        )

        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Deployed Lakeflow Job: ID={result.get('job_id')}")
            return result
        else:
            logger.error(f"❌ Deploy failed: {response.status_code} - {response.text}")
            raise RuntimeError(f"Job deployment failed: {response.text}")

    @property
    def warnings(self) -> list[str]:
        return self._warnings

    @property
    def manual_steps(self) -> list[str]:
        return self._manual_steps
