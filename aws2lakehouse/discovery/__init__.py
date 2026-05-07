from typing import Dict, List, Optional
import logging
import ast
logger = logging.getLogger(__name__)

"""Discovery module — Pipeline inventory, analysis, and wave planning."""
from aws2lakehouse.discovery.pipeline_inventory import PipelineInventory, PipelineRecord, Complexity
from aws2lakehouse.discovery.complexity_analyzer import ComplexityAnalyzer, ComplexityScore
from aws2lakehouse.discovery.wave_planner import WavePlanner, MigrationWave



class LatencyClassifier:
    """Classifies pipelines by data latency requirements."""
    
    BATCH = "batch"           # > 1 hour
    NEAR_REAL_TIME = "nrt"    # 1-60 minutes
    STREAMING = "streaming"   # < 1 minute
    
    @staticmethod
    def classify(schedule: str = None, processing_time: str = None, 
                 trigger_type: str = None) -> str:
        """Classify latency from schedule/processing characteristics."""
        if trigger_type in ("continuous", "kafka", "kinesis", "change_stream"):
            return LatencyClassifier.STREAMING
        if processing_time:
            # Parse processing time
            if "second" in processing_time or "1 minute" in processing_time:
                return LatencyClassifier.STREAMING
            if "minute" in processing_time:
                return LatencyClassifier.NEAR_REAL_TIME
        if schedule:
            # Parse cron-like schedule
            if "* * * * *" in schedule or "*/1" in schedule:
                return LatencyClassifier.STREAMING
            if any(f"*/{m}" in schedule for m in ["2","3","5","10","15","30"]):
                return LatencyClassifier.NEAR_REAL_TIME
        return LatencyClassifier.BATCH


class AirflowScanner:
    """Scans Airflow/MWAA environments for DAG inventory."""
    
    def __init__(self, airflow_url: str = None, dag_folder: str = None):
        self.airflow_url = airflow_url
        self.dag_folder = dag_folder
        self.dags: List[Dict] = []
    
    def scan_dag_folder(self, folder: str = None) -> List[Dict]:
        """Scan a folder of Airflow DAG files."""
        import glob
        folder = folder or self.dag_folder or "."
        dag_files = glob.glob(f"{folder}/**/*.py", recursive=True)
        
        for f in dag_files:
            try:
                with open(f) as fh:
                    source = fh.read()
                if "DAG" in source and ("airflow" in source or "dag_id" in source):
                    dag_info = self._parse_dag_file(f, source)
                    if dag_info:
                        self.dags.append(dag_info)
            except Exception as e:
                logger.warning(f"Failed to parse {f}: {e}")
        
        return self.dags
    
    def _parse_dag_file(self, filepath: str, source: str) -> Optional[Dict]:
        """Extract DAG metadata from a Python file."""
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return None
        
        dag_id = None
        schedule = None
        tasks = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                name = ""
                if isinstance(node.func, ast.Name):
                    name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    name = node.func.attr
                
                if name == "DAG":
                    for kw in node.keywords:
                        if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                            dag_id = kw.value.value
                        elif kw.arg in ("schedule_interval", "schedule") and isinstance(kw.value, ast.Constant):
                            schedule = kw.value.value
                    if node.args and isinstance(node.args[0], ast.Constant):
                        dag_id = dag_id or node.args[0].value
                
                if "Operator" in name or "Sensor" in name:
                    for kw in node.keywords:
                        if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                            tasks.append({"task_id": kw.value.value, "operator": name})
        
        if not dag_id:
            return None
        
        return {
            "dag_id": dag_id,
            "file": filepath,
            "schedule": schedule,
            "task_count": len(tasks),
            "tasks": tasks,
            "latency": LatencyClassifier.classify(schedule=schedule or ""),
            "operators_used": list(set(t["operator"] for t in tasks)),
        }
    
    def scan_airflow_api(self, base_url: str = None) -> List[Dict]:
        """Scan Airflow REST API for DAG metadata (requires connectivity)."""
        url = base_url or self.airflow_url
        if not url:
            logger.warning("No Airflow URL provided")
            return []
        
        try:
            import requests
            resp = requests.get(f"{url}/api/v1/dags", timeout=30)
            if resp.status_code == 200:
                for dag in resp.json().get("dags", []):
                    self.dags.append({
                        "dag_id": dag["dag_id"],
                        "schedule": dag.get("schedule_interval", {}).get("value"),
                        "is_paused": dag.get("is_paused", False),
                        "task_count": 0,  # Would need separate call
                        "file": dag.get("fileloc", ""),
                        "latency": LatencyClassifier.classify(schedule=dag.get("schedule_interval", {}).get("value", "")),
                    })
        except Exception as e:
            logger.warning(f"Airflow API scan failed: {e}")
        
        return self.dags


class SnowflakeScanner:
    """Scans Snowflake for table/pipeline metadata."""
    
    def __init__(self, account: str = "", warehouse: str = ""):
        self.account = account
        self.warehouse = warehouse
        self.tables: List[Dict] = []
        self.tasks: List[Dict] = []
    
    def scan_tables(self, database: str, schema: str = None) -> List[Dict]:
        """Scan Snowflake INFORMATION_SCHEMA for table metadata."""
        # Generates the SQL needed — actual execution requires snowflake-connector
        query = f"""
SELECT table_catalog, table_schema, table_name, table_type,
       row_count, bytes, created, last_altered
FROM {database}.information_schema.tables
{"WHERE table_schema = '" + schema + "'" if schema else ""}
ORDER BY last_altered DESC;
"""
        self.tables.append({"query": query, "database": database, "schema": schema})
        return self.tables
    
    def scan_tasks(self, database: str) -> List[Dict]:
        """Scan Snowflake Tasks (scheduled SQL)."""
        query = f"""
SELECT name, database_name, schema_name, schedule, state, 
       definition, predecessors, created_on
FROM {database}.information_schema.tasks
WHERE state = 'started';
"""
        self.tasks.append({"query": query, "database": database})
        return self.tasks
    
    def generate_scan_sql(self) -> str:
        """Generate SQL to run in Snowflake for discovery."""
        return """-- Snowflake Discovery Queries
-- Run these in Snowflake to extract pipeline metadata

-- 1. All tables with sizes
SELECT table_catalog, table_schema, table_name, row_count, bytes/1024/1024 as mb
FROM information_schema.tables WHERE table_type = 'BASE TABLE';

-- 2. Active tasks (scheduled pipelines)
SHOW TASKS;

-- 3. Recent query patterns (identifies pipelines)
SELECT query_type, database_name, schema_name, 
       COUNT(*) as executions, AVG(total_elapsed_time)/1000 as avg_seconds
FROM snowflake.account_usage.query_history
WHERE start_time > DATEADD(day, -7, current_timestamp())
GROUP BY 1, 2, 3 ORDER BY executions DESC LIMIT 100;

-- 4. Streams (CDC sources)
SHOW STREAMS;

-- 5. Pipes (continuous ingestion)
SHOW PIPES;
"""


class MongoDBScanner:
    """Scans MongoDB for collection metadata."""
    
    def __init__(self, connection_uri: str = ""):
        self.uri = connection_uri
        self.collections: List[Dict] = []
    
    def scan_collections(self, database: str) -> List[Dict]:
        """Scan MongoDB collections for migration assessment."""
        # Generates the JavaScript needed for mongo shell
        self.collections.append({"database": database, "status": "requires_connection"})
        return self.collections
    
    def generate_scan_script(self, database: str) -> str:
        """Generate mongo shell script for discovery."""
        return f"""// MongoDB Discovery Script
// Run: mongosh {database} < this_script.js

use {database};

// List all collections with stats
db.getCollectionNames().forEach(function(name) {{
    var stats = db.getCollection(name).stats();
    var indexes = db.getCollection(name).getIndexes();
    print(JSON.stringify({{
        collection: name,
        count: stats.count,
        size_mb: Math.round(stats.size / 1024 / 1024),
        avg_doc_size: stats.avgObjSize,
        indexes: indexes.length,
        capped: stats.capped || false
    }}));
}});

// Check change streams support (requires replica set)
try {{
    var cs = db.watch([], {{maxAwaitTimeMS: 100}});
    print("CHANGE_STREAMS: SUPPORTED");
    cs.close();
}} catch(e) {{
    print("CHANGE_STREAMS: NOT_SUPPORTED - " + e.message);
}}
"""


class PostgreSQLScanner:
    """Scans PostgreSQL for table/pipeline metadata."""
    
    def __init__(self, connection_string: str = ""):
        self.conn_string = connection_string
        self.tables: List[Dict] = []
    
    def generate_scan_sql(self) -> str:
        """Generate PostgreSQL discovery SQL."""
        return """-- PostgreSQL Discovery Queries

-- 1. All tables with sizes and row counts
SELECT schemaname, tablename, 
       pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as total_size,
       n_live_tup as approx_rows
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;

-- 2. Tables with CDC potential (has updated_at or similar)
SELECT table_schema, table_name, column_name
FROM information_schema.columns
WHERE column_name IN ('updated_at', 'modified_at', 'last_modified', 'changed_at', 'update_timestamp')
  AND table_schema NOT IN ('pg_catalog', 'information_schema');

-- 3. Foreign key relationships (dependency graph)
SELECT tc.table_schema, tc.table_name, kcu.column_name,
       ccu.table_schema AS ref_schema, ccu.table_name AS ref_table
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY';

-- 4. Logical replication slots (existing CDC)
SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots;

-- 5. Active connections (identify application patterns)
SELECT datname, usename, application_name, COUNT(*) 
FROM pg_stat_activity 
WHERE state = 'active' GROUP BY 1,2,3 ORDER BY 4 DESC;
"""
