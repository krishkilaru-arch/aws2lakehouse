"""
EMR Migrator — End-to-end migration of EMR jobs to Databricks.

Handles:
- Spark-submit command parsing
- PySpark/Scala code transformation
- JAR dependency analysis and replacement
- Hive metastore → Unity Catalog migration
- EMRFS → Databricks file access patterns
- Bootstrap actions → Init scripts
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path
import re
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class SparkSubmitConfig:
    """Parsed spark-submit command configuration."""
    main_class: str = ""
    main_script: str = ""  # .py or .jar
    jars: List[str] = field(default_factory=list)
    packages: List[str] = field(default_factory=list)
    py_files: List[str] = field(default_factory=list)
    files: List[str] = field(default_factory=list)
    conf: Dict[str, str] = field(default_factory=dict)
    driver_memory: str = "4g"
    executor_memory: str = "8g"
    executor_cores: int = 4
    num_executors: int = 4
    deploy_mode: str = "cluster"
    app_args: List[str] = field(default_factory=list)


@dataclass
class EMRMigrationResult:
    """Result of an EMR job migration."""
    success: bool
    source_script: str
    target_path: str
    cluster_config: Dict = field(default_factory=dict)
    code_transformations: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    manual_steps: List[str] = field(default_factory=list)
    jar_replacements: Dict[str, str] = field(default_factory=dict)


class SparkSubmitParser:
    """
    Parses EMR spark-submit commands into structured configuration.
    
    Handles both CLI-style and EMR Step JSON formats.
    """
    
    def parse_command(self, command: str) -> SparkSubmitConfig:
        """Parse a spark-submit command string."""
        config = SparkSubmitConfig()
        args = self._tokenize(command)
        
        i = 0
        while i < len(args):
            arg = args[i]
            
            if arg == "--class" and i + 1 < len(args):
                config.main_class = args[i + 1]
                i += 2
            elif arg == "--jars" and i + 1 < len(args):
                config.jars = [j.strip() for j in args[i + 1].split(",")]
                i += 2
            elif arg == "--packages" and i + 1 < len(args):
                config.packages = [p.strip() for p in args[i + 1].split(",")]
                i += 2
            elif arg == "--py-files" and i + 1 < len(args):
                config.py_files = [p.strip() for p in args[i + 1].split(",")]
                i += 2
            elif arg == "--files" and i + 1 < len(args):
                config.files = [f.strip() for f in args[i + 1].split(",")]
                i += 2
            elif arg == "--conf" and i + 1 < len(args):
                key, _, value = args[i + 1].partition("=")
                config.conf[key] = value
                i += 2
            elif arg == "--driver-memory" and i + 1 < len(args):
                config.driver_memory = args[i + 1]
                i += 2
            elif arg == "--executor-memory" and i + 1 < len(args):
                config.executor_memory = args[i + 1]
                i += 2
            elif arg == "--executor-cores" and i + 1 < len(args):
                config.executor_cores = int(args[i + 1])
                i += 2
            elif arg == "--num-executors" and i + 1 < len(args):
                config.num_executors = int(args[i + 1])
                i += 2
            elif arg == "--deploy-mode" and i + 1 < len(args):
                config.deploy_mode = args[i + 1]
                i += 2
            elif arg.endswith((".py", ".jar")) and not arg.startswith("--"):
                config.main_script = arg
                # Everything after is app args
                config.app_args = args[i + 1:]
                break
            else:
                i += 1
        
        return config
    
    def parse_emr_step(self, step_config: Dict) -> SparkSubmitConfig:
        """Parse an EMR Step JSON configuration."""
        hadoop_jar_step = step_config.get("HadoopJarStep", step_config.get("Config", {}))
        jar = hadoop_jar_step.get("Jar", "")
        args = hadoop_jar_step.get("Args", [])
        
        # If it's command-runner.jar, the real command is in args
        if "command-runner" in jar:
            command = " ".join(args)
            return self.parse_command(command)
        else:
            # Direct JAR execution
            config = SparkSubmitConfig(main_script=jar, app_args=args)
            return config
    
    def _tokenize(self, command: str) -> List[str]:
        """Tokenize command handling quotes."""
        tokens = []
        current = ""
        in_quotes = False
        quote_char = ""
        
        for char in command:
            if char in ('"', "'") and not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = ""
            elif char == " " and not in_quotes:
                if current:
                    tokens.append(current)
                    current = ""
            else:
                current += char
        
        if current:
            tokens.append(current)
        
        # Filter out spark-submit itself
        if tokens and "spark-submit" in tokens[0]:
            tokens = tokens[1:]
        
        return tokens


class JARAnalyzer:
    """
    Analyzes custom JARs and recommends Databricks alternatives.
    
    Common replacements:
    - aws-java-sdk → already included in DBR
    - hadoop-aws → already included in DBR  
    - delta-core → included in DBR (native Delta)
    - hive-exec → not needed (Unity Catalog)
    - Custom connectors → Databricks native connectors
    """
    
    # Known JAR replacements
    REPLACEMENTS = {
        "aws-java-sdk": "Built into Databricks Runtime (no action needed)",
        "hadoop-aws": "Built into Databricks Runtime (no action needed)",
        "delta-core": "Native Delta Lake in Databricks (remove dependency)",
        "delta-storage": "Native Delta Lake in Databricks (remove dependency)",
        "hive-exec": "Replaced by Unity Catalog (remove dependency)",
        "hive-metastore": "Replaced by Unity Catalog (remove dependency)",
        "spark-avro": "Built into Databricks Runtime (use format='avro')",
        "mysql-connector": "Use Databricks Lakehouse Federation or secrets",
        "postgresql": "Use Databricks Lakehouse Federation or secrets",
        "redshift-connector": "Use Databricks Redshift connector or Lakehouse Federation",
        "snowflake-spark": "Use Databricks Snowflake connector (net.snowflake:spark-snowflake)",
        "mongo-spark": "Use MongoDB Connector for Spark (included in DBR ML)",
        "kafka-clients": "Built into Databricks Runtime (Structured Streaming)",
        "spark-streaming-kafka": "Use Structured Streaming (built-in)",
        "elasticsearch-spark": "Available as library on cluster",
        "guava": "Built into Databricks Runtime",
        "jackson": "Built into Databricks Runtime",
        "slf4j": "Built into Databricks Runtime",
    }
    
    def analyze_jars(self, jar_paths: List[str]) -> Dict[str, Any]:
        """Analyze JARs and recommend actions."""
        results = {
            "total_jars": len(jar_paths),
            "auto_resolved": [],
            "needs_install": [],
            "needs_rewrite": [],
            "unknown": []
        }
        
        for jar_path in jar_paths:
            jar_name = Path(jar_path).stem.lower()
            
            # Check known replacements
            matched = False
            for known_jar, replacement in self.REPLACEMENTS.items():
                if known_jar in jar_name:
                    results["auto_resolved"].append({
                        "jar": jar_path,
                        "action": replacement
                    })
                    matched = True
                    break
            
            if not matched:
                # Check if it's a custom/internal JAR
                if any(prefix in jar_path for prefix in ["s3://", "internal", "custom"]):
                    results["needs_rewrite"].append({
                        "jar": jar_path,
                        "action": "Custom JAR — needs analysis and potential rewrite to Python/native Spark"
                    })
                else:
                    results["unknown"].append({
                        "jar": jar_path,
                        "action": "Search Maven Central or Databricks Libraries for equivalent"
                    })
        
        return results


class EMRMigrator:
    """
    End-to-end EMR job migration to Databricks.
    
    Usage:
        migrator = EMRMigrator(target_catalog="production")
        
        result = migrator.migrate_emr_step(
            step_config=emr_step,
            target_path="/Workspace/migrated/jobs/",
            source_code=script_content
        )
        
        # Or migrate from spark-submit command
        result = migrator.migrate_spark_submit(
            command="spark-submit --class com.app.Main --jars deps.jar app.jar --input s3://...",
            source_code=script_content,
            target_path="/Workspace/migrated/jobs/"
        )
    """
    
    def __init__(self, target_catalog: str = "production", photon_enabled: bool = True):
        self.target_catalog = target_catalog
        self.parser = SparkSubmitParser()
        self.jar_analyzer = JARAnalyzer()
        self.photon_enabled = photon_enabled
        
        # Import cluster mapper
        from aws2lakehouse.compute.cluster_mapper import ClusterMapper
        self.cluster_mapper = ClusterMapper(photon_enabled=photon_enabled)
    
    def migrate_emr_step(
        self,
        step_config: Dict,
        target_path: str,
        source_code: str = None,
        cluster_config: Dict = None
    ) -> EMRMigrationResult:
        """
        Migrate a single EMR step to Databricks.
        
        Args:
            step_config: EMR Step configuration (HadoopJarStep format)
            target_path: Target workspace path for migrated code
            source_code: Source code content (if available)
            cluster_config: EMR cluster configuration for compute mapping
        """
        # Parse spark-submit
        submit_config = self.parser.parse_emr_step(step_config)
        
        warnings = []
        manual_steps = []
        transformations = []
        
        # Analyze JARs
        all_jars = submit_config.jars + ([submit_config.main_script] if submit_config.main_script.endswith(".jar") else [])
        jar_analysis = self.jar_analyzer.analyze_jars(all_jars)
        
        if jar_analysis["needs_rewrite"]:
            for jar in jar_analysis["needs_rewrite"]:
                warnings.append(f"Custom JAR needs rewrite: {jar['jar']}")
                manual_steps.append(f"Rewrite {jar['jar']} as Python/native Spark")
        
        # Transform source code if available
        if source_code:
            transformed_code, code_warnings = self._transform_code(source_code)
            warnings.extend(code_warnings)
            transformations.append("Code transformed from EMR to Databricks patterns")
        
        # Map cluster configuration
        mapped_cluster = {}
        if cluster_config:
            from aws2lakehouse.compute.cluster_mapper import ClusterMapper
            mapper = ClusterMapper(photon_enabled=self.photon_enabled)
            db_cluster = mapper.map_emr_cluster(
                instance_type=cluster_config.get("instance_type", "m5.xlarge"),
                instance_count=cluster_config.get("instance_count", 4),
                workload_type=cluster_config.get("workload_type", "batch_etl")
            )
            mapped_cluster = db_cluster.to_api_payload()
        
        return EMRMigrationResult(
            success=True,
            source_script=submit_config.main_script,
            target_path=target_path,
            cluster_config=mapped_cluster,
            code_transformations=transformations,
            warnings=warnings,
            manual_steps=manual_steps,
            jar_replacements={
                jar["jar"]: jar["action"] 
                for jar in jar_analysis["auto_resolved"]
            }
        )
    
    def _transform_code(self, source_code: str) -> tuple:
        """Transform EMR PySpark code to Databricks patterns."""
        warnings = []
        code = source_code
        
        # 1. Remove SparkSession builder (Databricks provides spark)
        code = re.sub(
            r"spark\s*=\s*SparkSession\.builder.*?\.getOrCreate\(\)",
            "# spark is pre-configured in Databricks\nspark = spark  # noqa: provided by Databricks",
            code, flags=re.DOTALL
        )
        
        # 2. Replace Hive metastore references with Unity Catalog
        code = re.sub(
            r"\.enableHiveSupport\(\)",
            "  # Unity Catalog replaces Hive Metastore",
            code
        )
        
        # 3. Convert S3 paths to Unity Catalog volumes
        s3_paths = re.findall(r"s3[a]?://([\w-]+)/([\w/.-]+)", code)
        for bucket, path in s3_paths:
            volume_name = bucket.replace("-", "_")
            code = code.replace(
                f"s3://{bucket}/{path}",
                f"/Volumes/{self.target_catalog}/external/{volume_name}/{path}"
            )
            code = code.replace(
                f"s3a://{bucket}/{path}",
                f"/Volumes/{self.target_catalog}/external/{volume_name}/{path}"
            )
        
        if s3_paths:
            warnings.append(f"Converted {len(s3_paths)} S3 paths to Volume paths — verify Volume creation")
        
        # 4. Replace saveAsTable with Unity Catalog qualified names
        code = re.sub(
            r'\.saveAsTable\("([^"]+)"',
            lambda m: f'.saveAsTable("{self.target_catalog}.{m.group(1)}"' 
                if "." not in m.group(1) or m.group(1).count(".") == 1
                else f'.saveAsTable("{m.group(1)}"',
            code
        )
        
        # 5. Replace spark.sql("USE database") with catalog-qualified
        code = re.sub(
            r'spark\.sql\("USE\s+(\w+)"\)',
            f'spark.sql("USE CATALOG {self.target_catalog}")',
            code, flags=re.IGNORECASE
        )
        
        # 6. Remove EMRFS configurations
        code = re.sub(r".*fs\.s3[a]?\..*", "", code)
        code = re.sub(r".*emrfs.*", "", code, flags=re.IGNORECASE)
        
        # 7. Add Databricks header
        header = f"""# ==============================================================================
# MIGRATED FROM AWS EMR TO DATABRICKS
# Framework: aws2lakehouse v1.0
# Target Catalog: {self.target_catalog}
# ==============================================================================
# Review items:
#   - Verify Unity Catalog table/volume paths
#   - Validate JAR replacements (see jar_analysis)
#   - Test with Photon enabled for performance gains
# ==============================================================================

"""
        code = header + code
        
        return code, warnings
    
    def migrate_spark_submit(
        self,
        command: str,
        source_code: str = None,
        target_path: str = "/Workspace/migrated/",
        cluster_config: Dict = None
    ) -> EMRMigrationResult:
        """Migrate from a spark-submit command string."""
        config = self.parser.parse_command(command)
        
        # Convert to step format and delegate
        step_config = {
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit"] + command.split()[1:]  # Simplified
            }
        }
        
        return self.migrate_emr_step(
            step_config=step_config,
            target_path=target_path,
            source_code=source_code,
            cluster_config=cluster_config
        )
