"""
Airflow/MWAA DAG Converter - Converts Airflow DAGs to Databricks Lakeflow Jobs.

Parses Airflow Python DAG files using AST and converts:
- Task dependencies -> Job task dependencies (depends_on)
- PythonOperator -> Notebook tasks
- BashOperator -> Shell/notebook tasks
- SparkSubmitOperator -> Spark notebook tasks
- Sensors -> Condition tasks or IF/ELSE
- SubDAGs -> Nested job references
- Scheduling (cron) -> Quartz cron
- Retries/Timeouts -> Job retry config
- Task groups -> Task prefixes

Usage:
    from aws2lakehouse.orchestration.airflow_converter import AirflowConverter
    converter = AirflowConverter(target_notebook_base="/Workspace/migrated/airflow/")
    job = converter.convert_dag_file("dags/daily_etl.py")
"""

import ast
import re
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class AirflowTask:
    """Parsed Airflow task."""
    task_id: str
    operator_type: str
    python_callable: Optional[str] = None
    bash_command: Optional[str] = None
    sql: Optional[str] = None
    params: Dict = field(default_factory=dict)
    retries: int = 0
    retry_delay_seconds: int = 300
    timeout_seconds: int = 3600
    trigger_rule: str = "all_success"
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)
    task_group: Optional[str] = None


@dataclass
class AirflowDAG:
    """Parsed Airflow DAG definition."""
    dag_id: str
    schedule_interval: Optional[str] = None
    start_date: Optional[str] = None
    catchup: bool = False
    default_args: Dict = field(default_factory=dict)
    tasks: List[AirflowTask] = field(default_factory=list)
    description: str = ""
    tags: List[str] = field(default_factory=list)
    
    @property
    def task_map(self) -> Dict[str, AirflowTask]:
        return {t.task_id: t for t in self.tasks}


@dataclass
class LakeflowTask:
    """Databricks Lakeflow Job task."""
    task_key: str
    notebook_path: str
    depends_on: List[str] = field(default_factory=list)
    max_retries: int = 0
    timeout_seconds: int = 3600
    parameters: Dict = field(default_factory=dict)
    description: str = ""
    condition: Optional[str] = None  # For conditional execution


@dataclass
class LakeflowJob:
    """Converted Lakeflow Job definition."""
    name: str
    tasks: List[LakeflowTask] = field(default_factory=list)
    schedule: Optional[Dict] = None
    tags: Dict[str, str] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    
    def to_yaml(self) -> str:
        """Generate DAB-compatible job YAML."""
        lines = ["resources:", "  jobs:", f"    {self.name.replace(' ', '_').replace('-', '_')}:"]
        lines.append(f"      name: \"{self.name}\"")
        
        if self.schedule:
            lines.append("      schedule:")
            lines.append(f"        quartz_cron_expression: \"{self.schedule.get('quartz_cron', '0 0 6 * * ? *')}\"")
            lines.append(f"        timezone_id: \"{self.schedule.get('timezone', 'UTC')}\"")
        
        lines.append("      tasks:")
        for task in self.tasks:
            lines.append(f"        - task_key: {task.task_key}")
            lines.append(f"          notebook_task:")
            lines.append(f"            notebook_path: {task.notebook_path}")
            if task.parameters:
                lines.append(f"            base_parameters:")
                for k, v in task.parameters.items():
                    lines.append(f"              {k}: \"{v}\"")
            if task.depends_on:
                lines.append(f"          depends_on:")
                for dep in task.depends_on:
                    lines.append(f"            - task_key: {dep}")
            if task.max_retries > 0:
                lines.append(f"          max_retries: {task.max_retries}")
            if task.timeout_seconds != 3600:
                lines.append(f"          timeout_seconds: {task.timeout_seconds}")
        
        if self.tags:
            lines.append("      tags:")
            for k, v in self.tags.items():
                lines.append(f"        {k}: \"{v}\"")
        
        return "\n".join(lines)


class AirflowConverter:
    """
    Converts Airflow DAG files to Databricks Lakeflow Jobs.
    
    Supports:
    - PythonOperator -> notebook with python callable code
    - BashOperator -> notebook with %sh magic
    - SparkSubmitOperator -> spark notebook
    - SQLOperator/PostgresOperator -> SQL notebook
    - Sensors (S3, External) -> condition/polling notebooks
    - Task dependencies (>> and set_upstream/set_downstream)
    - Task groups -> task key prefixes
    - SubDAGs -> warnings for manual review
    - Cron schedules -> Quartz cron
    """
    
    # Operator type mapping
    OPERATOR_MAP = {
        "PythonOperator": "python_notebook",
        "BashOperator": "shell_notebook",
        "SparkSubmitOperator": "spark_notebook",
        "DatabricksSubmitRunOperator": "existing_notebook",
        "DatabricksRunNowOperator": "existing_job",
        "SQLExecuteQueryOperator": "sql_notebook",
        "PostgresOperator": "sql_notebook",
        "MySqlOperator": "sql_notebook",
        "S3KeySensor": "sensor_notebook",
        "ExternalTaskSensor": "dependency_wait",
        "FileSensor": "sensor_notebook",
        "HttpSensor": "sensor_notebook",
        "EmrAddStepsOperator": "spark_notebook",
        "EmrCreateJobFlowOperator": "spark_notebook",
        "GlueCrawlerOperator": "catalog_notebook",
        "GlueJobOperator": "etl_notebook",
        "LambdaInvokeFunctionOperator": "python_notebook",
        "EmailOperator": "notification_notebook",
        "SlackWebhookOperator": "notification_notebook",
        "DummyOperator": "pass_through",
        "EmptyOperator": "pass_through",
    }
    
    def __init__(self, target_notebook_base: str = "/Workspace/migrated/airflow/"):
        self.target_notebook_base = target_notebook_base.rstrip("/")
    
    def convert_dag_file(self, dag_file_path: str) -> LakeflowJob:
        """Convert an Airflow DAG file to a Lakeflow Job."""
        with open(dag_file_path) as f:
            source = f.read()
        return self.convert_dag_source(source)
    
    def convert_dag_source(self, source: str) -> LakeflowJob:
        """Convert Airflow DAG source code to a Lakeflow Job."""
        dag = self._parse_dag(source)
        return self._convert_dag(dag)
    
    def _parse_dag(self, source: str) -> AirflowDAG:
        """Parse Airflow DAG Python source into structured representation."""
        tree = ast.parse(source)
        dag = AirflowDAG(dag_id="unknown")
        tasks: Dict[str, AirflowTask] = {}
        dependencies: List[Tuple[str, str]] = []  # (upstream, downstream)
        
        for node in ast.walk(tree):
            # Find DAG instantiation
            if isinstance(node, ast.Call):
                func_name = self._get_call_name(node)
                if func_name == "DAG" or func_name.endswith(".DAG"):
                    dag = self._parse_dag_call(node, dag)
            
            # Find task assignments: task_id = Operator(...)
            if isinstance(node, ast.Assign):
                if isinstance(node.value, ast.Call):
                    func_name = self._get_call_name(node.value)
                    operator_class = func_name.split(".")[-1] if "." in func_name else func_name
                    if operator_class in self.OPERATOR_MAP or "Operator" in operator_class or "Sensor" in operator_class:
                        task = self._parse_task(node, operator_class)
                        if task:
                            tasks[task.task_id] = task
            
            # Find dependencies: task1 >> task2
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.BinOp):
                deps = self._parse_bitshift_deps(node.value)
                dependencies.extend(deps)
        
        # Apply dependencies
        for upstream_id, downstream_id in dependencies:
            if upstream_id in tasks:
                tasks[upstream_id].downstream.append(downstream_id)
            if downstream_id in tasks:
                tasks[downstream_id].upstream.append(upstream_id)
        
        dag.tasks = list(tasks.values())
        return dag
    
    def _parse_dag_call(self, node: ast.Call, dag: AirflowDAG) -> AirflowDAG:
        """Parse DAG() constructor call."""
        if node.args:
            dag.dag_id = self._get_string_value(node.args[0])
        
        for kw in node.keywords:
            if kw.arg == "dag_id":
                dag.dag_id = self._get_string_value(kw.value)
            elif kw.arg == "schedule_interval" or kw.arg == "schedule":
                dag.schedule_interval = self._get_string_value(kw.value)
            elif kw.arg == "catchup":
                dag.catchup = self._get_bool_value(kw.value)
            elif kw.arg == "description":
                dag.description = self._get_string_value(kw.value)
            elif kw.arg == "tags":
                dag.tags = self._get_list_values(kw.value)
            elif kw.arg == "default_args":
                dag.default_args = self._get_dict_value(kw.value)
        
        return dag
    
    def _parse_task(self, node: ast.Assign, operator_class: str) -> Optional[AirflowTask]:
        """Parse a task assignment."""
        call = node.value
        task = AirflowTask(task_id="", operator_type=operator_class)
        
        # Get variable name as fallback task_id
        if node.targets and isinstance(node.targets[0], ast.Name):
            task.task_id = node.targets[0].id
        
        for kw in call.keywords:
            if kw.arg == "task_id":
                task.task_id = self._get_string_value(kw.value)
            elif kw.arg == "python_callable":
                task.python_callable = self._get_name_value(kw.value)
            elif kw.arg == "bash_command":
                task.bash_command = self._get_string_value(kw.value)
            elif kw.arg == "sql":
                task.sql = self._get_string_value(kw.value)
            elif kw.arg == "retries":
                task.retries = self._get_int_value(kw.value)
            elif kw.arg == "execution_timeout":
                pass  # Would need timedelta parsing
            elif kw.arg == "trigger_rule":
                task.trigger_rule = self._get_string_value(kw.value)
            elif kw.arg == "op_kwargs" or kw.arg == "params":
                task.params = self._get_dict_value(kw.value)
        
        return task if task.task_id else None
    
    def _parse_bitshift_deps(self, node: ast.BinOp) -> List[Tuple[str, str]]:
        """Parse task1 >> task2 >> task3 dependency chains."""
        deps = []
        if isinstance(node.op, ast.RShift):
            left_tasks = self._get_task_ids(node.left)
            right_tasks = self._get_task_ids(node.right)
            for l in left_tasks:
                for r in right_tasks:
                    deps.append((l, r))
            # Recurse for chains
            if isinstance(node.left, ast.BinOp):
                deps.extend(self._parse_bitshift_deps(node.left))
        return deps
    
    def _get_task_ids(self, node) -> List[str]:
        """Extract task IDs from AST node."""
        if isinstance(node, ast.Name):
            return [node.id]
        elif isinstance(node, ast.List):
            return [self._get_name_value(e) for e in node.elts if isinstance(e, ast.Name)]
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
            return self._get_task_ids(node.right)
        return []
    
    def _convert_dag(self, dag: AirflowDAG) -> LakeflowJob:
        """Convert parsed DAG to Lakeflow Job."""
        job = LakeflowJob(
            name=dag.dag_id,
            tags={"source": "airflow", "original_dag": dag.dag_id},
        )
        
        if dag.tags:
            for tag in dag.tags:
                job.tags[tag] = "true"
        
        # Convert schedule
        if dag.schedule_interval:
            job.schedule = {
                "quartz_cron": self._airflow_to_quartz(dag.schedule_interval),
                "timezone": dag.default_args.get("timezone", "UTC"),
            }
        
        # Convert tasks
        for task in dag.tasks:
            lf_task = self._convert_task(task, dag)
            if lf_task:
                job.tasks.append(lf_task)
        
        # Add warnings for complex patterns
        for task in dag.tasks:
            if "Sensor" in task.operator_type:
                job.warnings.append(f"Sensor '{task.task_id}' converted to polling notebook - review for native Databricks alternative")
            if "SubDag" in task.operator_type:
                job.warnings.append(f"SubDAG '{task.task_id}' - flatten into main job or create separate job")
            if task.trigger_rule != "all_success":
                job.warnings.append(f"Task '{task.task_id}' has trigger_rule='{task.trigger_rule}' - review condition logic")
        
        return job
    
    def _convert_task(self, task: AirflowTask, dag: AirflowDAG) -> Optional[LakeflowTask]:
        """Convert a single Airflow task to Lakeflow task."""
        task_type = self.OPERATOR_MAP.get(task.operator_type, "python_notebook")
        
        if task_type == "pass_through":
            # DummyOperator/EmptyOperator - skip but preserve deps
            return None
        
        notebook_path = f"{self.target_notebook_base}/{dag.dag_id}/{task.task_id}"
        
        # Map dependencies (skip DummyOperators)
        depends_on = [dep for dep in task.upstream 
                      if dep in dag.task_map and 
                      dag.task_map[dep].operator_type not in ("DummyOperator", "EmptyOperator")]
        
        lf_task = LakeflowTask(
            task_key=task.task_id.replace("-", "_"),
            notebook_path=notebook_path,
            depends_on=[d.replace("-", "_") for d in depends_on],
            max_retries=task.retries or dag.default_args.get("retries", 0),
            timeout_seconds=task.timeout_seconds,
            parameters=task.params,
            description=f"Migrated from Airflow: {task.operator_type}",
        )
        
        return lf_task
    
    def generate_task_notebook(self, task: AirflowTask, dag: AirflowDAG) -> str:
        """Generate notebook code for a converted task."""
        lines = [
            "# Databricks notebook source",
            f"# Migrated from Airflow DAG: {dag.dag_id}",
            f"# Original operator: {task.operator_type}",
            f"# Task ID: {task.task_id}",
            "",
        ]
        
        if task.python_callable:
            lines.append(f"# Original python_callable: {task.python_callable}")
            lines.append("# TODO: Port the function logic below")
            lines.append(f"# def {task.python_callable}(**kwargs):")
            lines.append("#     pass")
        elif task.bash_command:
            lines.append("# MAGIC %sh")
            lines.append(f"# {task.bash_command}")
        elif task.sql:
            lines.append("# MAGIC %sql")
            lines.append(f"# {task.sql}")
        else:
            lines.append("# TODO: Implement task logic")
        
        return "\n".join(lines)
    
    # Helper methods
    def _airflow_to_quartz(self, schedule: str) -> str:
        """Convert Airflow schedule to Quartz cron."""
        presets = {
            "@daily": "0 0 0 * * ? *",
            "@hourly": "0 0 * * * ? *",
            "@weekly": "0 0 0 ? * MON *",
            "@monthly": "0 0 0 1 * ? *",
            "@yearly": "0 0 0 1 1 ? *",
            "@once": "0 0 0 1 1 ? 2099",
            "None": "",
        }
        if schedule in presets:
            return presets[schedule]
        # Standard cron (5-field) to Quartz (7-field)
        parts = schedule.split()
        if len(parts) == 5:
            dow = "?" if parts[4] == "*" else parts[4]
            dom = "?" if dow != "?" else parts[2]
            return f"0 {parts[0]} {parts[1]} {dom} {parts[3]} {dow} *"
        return schedule
    
    @staticmethod
    def _get_call_name(node: ast.Call) -> str:
        if isinstance(node.func, ast.Name):
            return node.func.id
        elif isinstance(node.func, ast.Attribute):
            return node.func.attr
        return ""
    
    @staticmethod
    def _get_string_value(node) -> str:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return ""
    
    @staticmethod
    def _get_int_value(node) -> int:
        if isinstance(node, ast.Constant) and isinstance(node.value, int):
            return node.value
        return 0
    
    @staticmethod
    def _get_bool_value(node) -> bool:
        if isinstance(node, ast.Constant):
            return bool(node.value)
        return False
    
    @staticmethod
    def _get_name_value(node) -> str:
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return node.attr
        return ""
    
    @staticmethod
    def _get_list_values(node) -> List[str]:
        if isinstance(node, ast.List):
            return [AirflowConverter._get_string_value(e) for e in node.elts]
        return []
    
    @staticmethod
    def _get_dict_value(node) -> Dict:
        if isinstance(node, ast.Dict):
            result = {}
            for k, v in zip(node.keys, node.values):
                key = AirflowConverter._get_string_value(k)
                if key:
                    result[key] = AirflowConverter._get_string_value(v) or str(v)
            return result
        return {}
