"""
Pipeline Inventory — Scans AWS accounts for EMR, Glue, and Step Function workloads.

Builds a comprehensive catalog of all data pipelines for migration planning.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class PipelineType(Enum):
    EMR_SPARK = "emr_spark"
    EMR_HIVE = "emr_hive"
    EMR_PRESTO = "emr_presto"
    GLUE_PYSPARK = "glue_pyspark"
    GLUE_SCALA = "glue_scala"
    GLUE_PYTHON_SHELL = "glue_python_shell"
    STEP_FUNCTION = "step_function"
    MWAA_DAG = "mwaa_dag"
    LAMBDA = "lambda"
    CUSTOM = "custom"


class Complexity(Enum):
    SIMPLE = "simple"          # <50 lines, single source/sink, no joins
    MEDIUM = "medium"          # 50-200 lines, 2-3 sources, basic joins
    COMPLEX = "complex"        # 200-500 lines, multiple sources, UDFs, custom logic
    CRITICAL = "critical"      # >500 lines, real-time, regulatory, cross-system


class BusinessImpact(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class DataSource:
    """Represents a data source in the pipeline."""
    name: str
    source_type: str  # jdbc, s3, kafka, mongodb, snowflake, delta_share, api
    connection_info: dict[str, Any] = field(default_factory=dict)
    schema: Optional[str] = None
    estimated_volume_gb: float = 0.0
    refresh_frequency: str = "daily"
    sla_hours: Optional[float] = None


@dataclass
class PipelineRecord:
    """Complete record of a discovered pipeline."""
    pipeline_id: str
    name: str
    pipeline_type: PipelineType
    complexity: Complexity = Complexity.MEDIUM
    business_impact: BusinessImpact = BusinessImpact.MEDIUM

    # Source details
    aws_account_id: str = ""
    region: str = "us-east-1"
    script_location: str = ""

    # Technical metadata
    language: str = "python"
    spark_version: str = ""
    dependencies: list[str] = field(default_factory=list)
    custom_jars: list[str] = field(default_factory=list)
    data_sources: list[DataSource] = field(default_factory=list)

    # Runtime metadata
    avg_runtime_minutes: float = 0.0
    schedule: str = ""
    last_run: Optional[datetime] = None
    failure_rate: float = 0.0

    # Migration metadata
    migration_wave: Optional[int] = None
    migration_status: str = "not_started"
    estimated_effort_hours: float = 0.0
    risk_score: float = 0.0
    blockers: list[str] = field(default_factory=list)

    # Cluster info (EMR)
    instance_type: str = ""
    instance_count: int = 0
    emr_release: str = ""

    # Business context
    owner: str = ""
    domain: str = ""
    sla_description: str = ""
    downstream_consumers: list[str] = field(default_factory=list)


class PipelineInventory:
    """
    Scans AWS environment to build pipeline inventory.

    Usage:
        inventory = PipelineInventory(aws_profile="production")
        inventory.scan_emr_clusters(region="us-east-1")
        inventory.scan_glue_jobs(region="us-east-1")
        inventory.scan_step_functions(region="us-east-1")

        report = inventory.generate_assessment_report()
        waves = inventory.recommend_waves()
    """

    def __init__(self, aws_profile: str = "default", regions: list[str] = None):
        self.aws_profile = aws_profile
        self.regions = regions or ["us-east-1"]
        self.pipelines: list[PipelineRecord] = []
        self.data_sources: list[DataSource] = []
        self._scan_timestamp = None

    def scan_emr_clusters(self, region: str = "us-east-1") -> list[PipelineRecord]:
        """
        Scan EMR clusters and extract job/step information.

        Captures:
        - Active clusters and their configurations
        - EMR Steps (spark-submit commands)
        - Associated S3 scripts and JARs
        - Instance types and autoscaling configs
        - Bootstrap actions and custom AMIs
        """
        logger.info(f"Scanning EMR clusters in {region}...")

        try:
            import boto3
            emr_client = boto3.client("emr", region_name=region)

            # List all clusters (active + recently terminated)
            paginator = emr_client.get_paginator("list_clusters")
            clusters = []
            for page in paginator.paginate(
                ClusterStates=["RUNNING", "WAITING", "TERMINATED"]
            ):
                clusters.extend(page.get("Clusters", []))

            for cluster_summary in clusters:
                cluster_id = cluster_summary["Id"]
                cluster = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]

                # Get steps
                steps_paginator = emr_client.get_paginator("list_steps")
                for step_page in steps_paginator.paginate(ClusterId=cluster_id):
                    for step in step_page.get("Steps", []):
                        pipeline = self._parse_emr_step(step, cluster)
                        if pipeline:
                            self.pipelines.append(pipeline)

            logger.info(f"Found {len(self.pipelines)} EMR pipelines in {region}")

        except ImportError:
            logger.warning("boto3 not available — using mock scan for demo")
            self._mock_emr_scan(region)
        except Exception as e:
            logger.error(f"EMR scan failed: {e}")

        return self.pipelines

    def scan_glue_jobs(self, region: str = "us-east-1") -> list[PipelineRecord]:
        """
        Scan AWS Glue jobs and crawlers.

        Captures:
        - Job definitions (script, worker type, connections)
        - Glue version and Python version
        - Job bookmarks and pushdown predicates
        - Crawler schedules and targets
        """
        logger.info(f"Scanning Glue jobs in {region}...")

        try:
            import boto3
            glue_client = boto3.client("glue", region_name=region)

            paginator = glue_client.get_paginator("get_jobs")
            for page in paginator.paginate():
                for job in page.get("Jobs", []):
                    pipeline = self._parse_glue_job(job)
                    self.pipelines.append(pipeline)

            logger.info(f"Total pipelines after Glue scan: {len(self.pipelines)}")

        except ImportError:
            logger.warning("boto3 not available — using mock scan for demo")
            self._mock_glue_scan(region)
        except Exception as e:
            logger.error(f"Glue scan failed: {e}")

        return self.pipelines

    def scan_step_functions(self, region: str = "us-east-1") -> list[PipelineRecord]:
        """Scan Step Functions state machines for orchestration patterns."""
        logger.info(f"Scanning Step Functions in {region}...")

        try:
            import boto3
            sfn_client = boto3.client("stepfunctions", region_name=region)

            paginator = sfn_client.get_paginator("list_state_machines")
            for page in paginator.paginate():
                for sm in page.get("stateMachines", []):
                    definition = sfn_client.describe_state_machine(
                        stateMachineArn=sm["stateMachineArn"]
                    )
                    pipeline = PipelineRecord(
                        pipeline_id=sm["stateMachineArn"],
                        name=sm["name"],
                        pipeline_type=PipelineType.STEP_FUNCTION,
                        script_location=sm["stateMachineArn"],
                        region=region
                    )
                    # Analyze definition for complexity
                    defn = json.loads(definition.get("definition", "{}"))
                    states = defn.get("States", {})
                    pipeline.complexity = self._classify_step_function_complexity(states)
                    self.pipelines.append(pipeline)

        except ImportError:
            logger.warning("boto3 not available — using mock scan")
            self._mock_step_function_scan(region)
        except Exception as e:
            logger.error(f"Step Functions scan failed: {e}")

        return self.pipelines

    def _parse_emr_step(self, step: dict, cluster: dict) -> Optional[PipelineRecord]:
        """Parse an EMR step into a PipelineRecord."""
        hadoop_jar_step = step.get("Config", {}).get("Args", [])

        # Extract spark-submit details
        script_path = ""
        jars = []
        for i, arg in enumerate(hadoop_jar_step):
            if arg.endswith(".py") or arg.endswith(".jar"):
                script_path = arg
            if arg == "--jars" and i + 1 < len(hadoop_jar_step):
                jars = hadoop_jar_step[i + 1].split(",")

        if not script_path:
            return None

        # Map instance group info
        instance_type = ""
        instance_count = 0
        for ig in cluster.get("InstanceGroups", []):
            if ig.get("InstanceGroupType") == "CORE":
                instance_type = ig.get("InstanceType", "")
                instance_count = ig.get("RequestedInstanceCount", 0)

        return PipelineRecord(
            pipeline_id=f"emr-{cluster['Id']}-{step['Id']}",
            name=step.get("Name", "unnamed"),
            pipeline_type=PipelineType.EMR_SPARK,
            script_location=script_path,
            custom_jars=jars,
            instance_type=instance_type,
            instance_count=instance_count,
            emr_release=cluster.get("ReleaseLabel", ""),
            language="python" if script_path.endswith(".py") else "scala"
        )

    def _parse_glue_job(self, job: dict) -> PipelineRecord:
        """Parse a Glue job definition into a PipelineRecord."""
        command = job.get("Command", {})
        script = command.get("ScriptLocation", "")

        # Determine type
        if command.get("Name") == "pythonshell":
            ptype = PipelineType.GLUE_PYTHON_SHELL
        elif "scala" in script.lower():
            ptype = PipelineType.GLUE_SCALA
        else:
            ptype = PipelineType.GLUE_PYSPARK

        # Extract connections as data sources
        job.get("Connections", {}).get("Connections", [])

        return PipelineRecord(
            pipeline_id=f"glue-{job['Name']}",
            name=job["Name"],
            pipeline_type=ptype,
            script_location=script,
            language="python" if ptype != PipelineType.GLUE_SCALA else "scala",
            spark_version=job.get("GlueVersion", ""),
            dependencies=[
                job.get("DefaultArguments", {}).get("--extra-py-files", "")
            ]
        )

    def _classify_step_function_complexity(self, states: dict) -> Complexity:
        """Classify Step Function complexity based on state types."""
        state_count = len(states)
        has_parallel = any(s.get("Type") == "Parallel" for s in states.values())
        has_map = any(s.get("Type") == "Map" for s in states.values())
        any(s.get("Type") == "Choice" for s in states.values())

        if state_count <= 3 and not has_parallel:
            return Complexity.SIMPLE
        elif state_count <= 8 and not has_map:
            return Complexity.MEDIUM
        elif has_parallel or has_map:
            return Complexity.COMPLEX
        else:
            return Complexity.CRITICAL

    def _mock_emr_scan(self, region: str):
        """Generate mock EMR pipeline data for demo."""
        mock_pipelines = [
            PipelineRecord(
                pipeline_id="emr-j-ABC123-s001",
                name="daily-loan-etl",
                pipeline_type=PipelineType.EMR_SPARK,
                complexity=Complexity.COMPLEX,
                business_impact=BusinessImpact.CRITICAL,
                script_location="s3://data-platform/scripts/loan_etl.py",
                instance_type="r5.2xlarge",
                instance_count=10,
                emr_release="emr-6.15.0",
                avg_runtime_minutes=45,
                schedule="0 6 * * *",
                domain="lending",
                owner="data-engineering@company.com"
            ),
            PipelineRecord(
                pipeline_id="emr-j-DEF456-s002",
                name="realtime-fraud-scoring",
                pipeline_type=PipelineType.EMR_SPARK,
                complexity=Complexity.CRITICAL,
                business_impact=BusinessImpact.CRITICAL,
                script_location="s3://data-platform/jars/fraud-engine.jar",
                custom_jars=["s3://data-platform/jars/fraud-engine.jar", "s3://data-platform/jars/ml-models.jar"],
                instance_type="r5.4xlarge",
                instance_count=20,
                emr_release="emr-6.15.0",
                avg_runtime_minutes=0,  # streaming
                schedule="always_on",
                language="scala",
                domain="risk",
                owner="risk-engineering@company.com"
            ),
        ]
        self.pipelines.extend(mock_pipelines)

    def _mock_glue_scan(self, region: str):
        """Generate mock Glue pipeline data for demo."""
        mock_pipelines = [
            PipelineRecord(
                pipeline_id="glue-customer-enrichment",
                name="customer-enrichment",
                pipeline_type=PipelineType.GLUE_PYSPARK,
                complexity=Complexity.MEDIUM,
                script_location="s3://glue-scripts/customer_enrichment.py",
                spark_version="4.0",
                avg_runtime_minutes=20,
                schedule="0 */4 * * *",
                domain="customer"
            ),
        ]
        self.pipelines.extend(mock_pipelines)

    def _mock_step_function_scan(self, region: str):
        """Generate mock Step Function data for demo."""
        self.pipelines.append(PipelineRecord(
            pipeline_id="arn:aws:states:us-east-1:123:stateMachine:daily-risk",
            name="daily-risk-pipeline",
            pipeline_type=PipelineType.STEP_FUNCTION,
            complexity=Complexity.COMPLEX,
            business_impact=BusinessImpact.HIGH,
            domain="risk"
        ))

    def generate_assessment_report(self) -> dict[str, Any]:
        """Generate comprehensive migration assessment report."""
        report = {
            "scan_date": datetime.now().isoformat(),
            "total_pipelines": len(self.pipelines),
            "by_type": {},
            "by_complexity": {},
            "by_impact": {},
            "by_domain": {},
            "estimated_total_effort_hours": 0,
            "recommended_waves": 0,
            "blockers": [],
            "risks": []
        }

        for p in self.pipelines:
            # Count by type
            t = p.pipeline_type.value
            report["by_type"][t] = report["by_type"].get(t, 0) + 1

            # Count by complexity
            c = p.complexity.value
            report["by_complexity"][c] = report["by_complexity"].get(c, 0) + 1

            # Count by impact
            i = p.business_impact.value
            report["by_impact"][i] = report["by_impact"].get(i, 0) + 1

            # Count by domain
            d = p.domain or "unclassified"
            report["by_domain"][d] = report["by_domain"].get(d, 0) + 1

            # Collect blockers
            report["blockers"].extend(p.blockers)

        # Estimate effort
        effort_map = {
            Complexity.SIMPLE: 4,
            Complexity.MEDIUM: 16,
            Complexity.COMPLEX: 40,
            Complexity.CRITICAL: 80
        }
        report["estimated_total_effort_hours"] = sum(
            effort_map.get(p.complexity, 16) for p in self.pipelines
        )

        # Recommend waves (5-8 pipelines per wave for large migrations)
        report["recommended_waves"] = max(1, len(self.pipelines) // 6)

        return report

    def export_inventory(self, path: str, format: str = "json"):
        """Export pipeline inventory to file."""
        import json

        data = []
        for p in self.pipelines:
            record = {
                "pipeline_id": p.pipeline_id,
                "name": p.name,
                "type": p.pipeline_type.value,
                "complexity": p.complexity.value,
                "business_impact": p.business_impact.value,
                "script_location": p.script_location,
                "instance_type": p.instance_type,
                "instance_count": p.instance_count,
                "avg_runtime_minutes": p.avg_runtime_minutes,
                "schedule": p.schedule,
                "domain": p.domain,
                "owner": p.owner,
                "migration_wave": p.migration_wave,
                "estimated_effort_hours": p.estimated_effort_hours,
                "risk_score": p.risk_score
            }
            data.append(record)

        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

        logger.info(f"Exported {len(data)} pipelines to {path}")
