"""
Ingestion Patterns — Standardized data ingestion for Databricks Lakehouse.

Provides ready-to-use patterns for:
- Auto Loader (file-based ingestion with schema evolution)
- Delta Share with Change Data Feed (CDF)
- Kafka Structured Streaming
- MongoDB Change Streams
- PostgreSQL CDC (via Debezium or JDBC)
- Snowflake connector
- Custom API ingestion
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class IngestionConfig:
    """Base configuration for ingestion patterns."""
    source_name: str
    target_catalog: str = "production"
    target_schema: str = "bronze"
    target_table: str = ""
    checkpoint_path: str = ""
    schema_evolution: bool = True
    merge_schema: bool = True
    trigger: str = "availableNow"  # availableNow, processingTime, continuous


class AutoLoaderPattern:
    """
    Auto Loader ingestion pattern for file-based data sources.
    
    Supports: CSV, JSON, Parquet, Avro, ORC, Binary, Text
    Features: Schema evolution, rescue column, file notification mode
    
    Usage:
        loader = AutoLoaderPattern(
            source_path="/Volumes/production/raw/orders/",
            target_table="production.bronze.orders",
            file_format="json",
            schema_evolution=True
        )
        
        # Generate notebook code
        code = loader.generate_notebook()
        
        # Or get Spark code directly
        df = loader.create_stream(spark)
    """
    
    def __init__(
        self,
        source_path: str,
        target_table: str,
        file_format: str = "json",
        schema_evolution: bool = True,
        schema_hints: Dict[str, str] = None,
        checkpoint_path: str = None,
        trigger: str = "availableNow",
        rescue_column: bool = True,
        notification_mode: bool = False,
        partition_columns: List[str] = None
    ):
        self.source_path = source_path
        self.target_table = target_table
        self.file_format = file_format
        self.schema_evolution = schema_evolution
        self.schema_hints = schema_hints or {}
        self.checkpoint_path = checkpoint_path or f"/checkpoints/{target_table.replace('.', '/')}"
        self.trigger = trigger
        self.rescue_column = rescue_column
        self.notification_mode = notification_mode
        self.partition_columns = partition_columns or []
    
    def generate_code(self) -> str:
        """Generate PySpark code for Auto Loader ingestion."""
        
        options = [
            f'    "cloudFiles.format": "{self.file_format}"',
            f'    "cloudFiles.schemaLocation": "{self.checkpoint_path}/schema"',
        ]
        
        if self.schema_evolution:
            options.append('    "cloudFiles.schemaEvolutionMode": "addNewColumns"')
        
        if self.rescue_column:
            options.append('    "cloudFiles.rescuedDataColumn": "_rescued_data"')
        
        if self.notification_mode:
            options.append('    "cloudFiles.useNotifications": "true"')
        
        if self.schema_hints:
            hints = ", ".join(f"{k} {v}" for k, v in self.schema_hints.items())
            options.append(f'    "cloudFiles.schemaHints": "{hints}"')
        
        # Format-specific options
        if self.file_format == "csv":
            options.append('    "header": "true"')
            options.append('    "inferSchema": "true"')
        elif self.file_format == "json":
            options.append('    "multiLine": "true"')
        
        options_str = ",\n".join(options)
        
        # Trigger
        if self.trigger == "availableNow":
            trigger_str = ".trigger(availableNow=True)"
        elif self.trigger.startswith("processingTime"):
            trigger_str = f'.trigger(processingTime="{self.trigger.split("=")[1] if "=" in self.trigger else "10 seconds"}")'
        else:
            trigger_str = ".trigger(availableNow=True)"
        
        # Write options
        write_opts = []
        if self.schema_evolution:
            write_opts.append('    .option("mergeSchema", "true")')
        if self.partition_columns:
            cols = ", ".join(f'"{c}"' for c in self.partition_columns)
            write_opts.append(f'    .partitionBy({cols})')
        
        write_opts_str = "\n".join(write_opts)
        
        code = f'''# Auto Loader Ingestion: {self.target_table}
# Source: {self.source_path}
# Format: {self.file_format} | Schema Evolution: {self.schema_evolution}

df = (spark.readStream
    .format("cloudFiles")
    .options({{
{options_str}
    }})
    .load("{self.source_path}")
)

# Add ingestion metadata
from pyspark.sql import functions as F

df = (df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# Write to Delta table
(df.writeStream
    .format("delta")
    .outputMode("append")
{write_opts_str}
    .option("checkpointLocation", "{self.checkpoint_path}")
    {trigger_str}
    .toTable("{self.target_table}")
)
'''
        return code


class DeltaShareCDF:
    """
    Delta Share with Change Data Feed (CDF) pattern.
    
    Enables cross-organization data sharing with change tracking.
    
    Usage:
        cdf = DeltaShareCDF(
            share_name="vendor_data_share",
            schema_name="vendor_schema",
            table_name="transactions",
            target_table="production.bronze.vendor_transactions"
        )
        code = cdf.generate_code()
    """
    
    def __init__(
        self,
        share_name: str,
        schema_name: str,
        table_name: str,
        target_table: str,
        profile_path: str = "/Volumes/production/config/delta_sharing/",
        cdf_enabled: bool = True,
        starting_version: int = 0
    ):
        self.share_name = share_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.target_table = target_table
        self.profile_path = profile_path
        self.cdf_enabled = cdf_enabled
        self.starting_version = starting_version
    
    def generate_code(self) -> str:
        """Generate code for Delta Share CDF ingestion."""
        
        if self.cdf_enabled:
            return f'''# Delta Share with Change Data Feed (CDF)
# Share: {self.share_name}.{self.schema_name}.{self.table_name}
# Target: {self.target_table}

# Read CDF stream from Delta Share
df = (spark.readStream
    .format("deltaSharing")
    .option("readChangeFeed", "true")
    .option("startingVersion", {self.starting_version})
    .table("{self.share_name}.{self.schema_name}.{self.table_name}")
)

# Process changes (insert, update, delete)
from pyspark.sql import functions as F

# Add processing metadata
df = (df
    .withColumn("_processed_at", F.current_timestamp())
    .withColumn("_change_type", F.col("_change_type"))
)

# Merge into target (upsert pattern)
from delta.tables import DeltaTable

def upsert_to_delta(batch_df, batch_id):
    target = DeltaTable.forName(spark, "{self.target_table}")
    
    (target.alias("t")
        .merge(batch_df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll(condition="s._change_type = 'update_postimage'")
        .whenNotMatchedInsertAll(condition="s._change_type IN ('insert', 'update_postimage')")
        .whenMatchedDelete(condition="s._change_type = 'delete'")
        .execute()
    )

# Write stream with merge
(df.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/checkpoints/{self.target_table.replace('.', '/')}")
    .trigger(availableNow=True)
    .start()
)
'''
        else:
            return f'''# Delta Share Batch Read
df = spark.read.format("deltaSharing").table(
    "{self.share_name}.{self.schema_name}.{self.table_name}"
)
df.write.format("delta").mode("overwrite").saveAsTable("{self.target_table}")
'''


class KafkaIngestion:
    """Kafka Structured Streaming ingestion to Bronze layer."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        target_table: str,
        schema_registry_url: str = None,
        consumer_group: str = None,
        starting_offsets: str = "latest",
        value_format: str = "json"  # json, avro, protobuf
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.target_table = target_table
        self.schema_registry_url = schema_registry_url
        self.consumer_group = consumer_group
        self.starting_offsets = starting_offsets
        self.value_format = value_format
    
    def generate_code(self) -> str:
        """Generate Kafka ingestion code."""
        return f'''# Kafka Structured Streaming Ingestion
# Topic: {self.topic} → {self.target_table}

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Read from Kafka
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "{self.bootstrap_servers}")
    .option("subscribe", "{self.topic}")
    .option("startingOffsets", "{self.starting_offsets}")
    .option("kafka.group.id", "{self.consumer_group or self.target_table.replace('.', '_')}")
    .load()
)

# Parse value (assuming JSON)
parsed_df = (kafka_df
    .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.from_json(F.col("value").cast("string"), schema).alias("data"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp")
    )
    .select("kafka_key", "data.*", "topic", "partition", "offset", "kafka_timestamp")
    .withColumn("_ingested_at", F.current_timestamp())
)

# Write to Bronze
(parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/{self.target_table.replace('.', '/')}")
    .trigger(processingTime="10 seconds")
    .toTable("{self.target_table}")
)
'''


class ConnectorFactory:
    """
    Factory for creating data source connectors.
    
    Supported: MongoDB, PostgreSQL, Snowflake, MySQL, Oracle, Elasticsearch
    """
    
    CONNECTOR_TEMPLATES = {
        "mongodb": '''# MongoDB Ingestion via Spark Connector
df = (spark.read
    .format("mongodb")
    .option("connection.uri", dbutils.secrets.get("{{scope}}", "mongodb_uri"))
    .option("database", "{{database}}")
    .option("collection", "{{collection}}")
    .load()
)
df.write.format("delta").mode("overwrite").saveAsTable("{{target_table}}")
''',
        "postgresql": '''# PostgreSQL JDBC Ingestion
df = (spark.read
    .format("jdbc")
    .option("url", dbutils.secrets.get("{{scope}}", "pg_url"))
    .option("dbtable", "{{source_table}}")
    .option("user", dbutils.secrets.get("{{scope}}", "pg_user"))
    .option("password", dbutils.secrets.get("{{scope}}", "pg_password"))
    .option("driver", "org.postgresql.Driver")
    .option("fetchsize", "10000")
    .option("partitionColumn", "{{partition_col}}")
    .option("lowerBound", "{{lower}}")
    .option("upperBound", "{{upper}}")
    .option("numPartitions", "{{num_partitions}}")
    .load()
)
df.write.format("delta").mode("overwrite").saveAsTable("{{target_table}}")
''',
        "snowflake": '''# Snowflake Ingestion
options = {
    "sfUrl": dbutils.secrets.get("{{scope}}", "sf_url"),
    "sfUser": dbutils.secrets.get("{{scope}}", "sf_user"),
    "sfPassword": dbutils.secrets.get("{{scope}}", "sf_password"),
    "sfDatabase": "{{database}}",
    "sfSchema": "{{schema}}",
    "sfWarehouse": "{{warehouse}}"
}

df = (spark.read
    .format("snowflake")
    .options(**options)
    .option("query", "SELECT * FROM {{source_table}}")
    .load()
)
df.write.format("delta").mode("overwrite").saveAsTable("{{target_table}}")
'''
    }
    
    @classmethod
    def generate(cls, connector_type: str, params: Dict[str, str]) -> str:
        """Generate connector code with parameters filled in."""
        template = cls.CONNECTOR_TEMPLATES.get(connector_type, "")
        if not template:
            raise ValueError(f"Unknown connector type: {connector_type}")
        
        for key, value in params.items():
            template = template.replace("{{" + key + "}}", value)
        
        return template
