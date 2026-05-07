"""
Redshift Loader — Load data from S3 into Redshift using COPY/MERGE.

Supports: full refresh, append, upsert (MERGE), and SCD Type 2.
"""
import boto3
import json
import redshift_connector
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class RedshiftLoader:
    """Load data into Redshift from S3 Parquet."""
    
    def __init__(self, cluster_id: str = "shopmax-analytics"):
        self.cluster_id = cluster_id
        self._conn = None
    
    @property
    def conn(self):
        if self._conn is None:
            creds = self._get_credentials()
            self._conn = redshift_connector.connect(
                host=creds["host"], port=int(creds["port"]),
                database=creds["database"],
                user=creds["username"], password=creds["password"]
            )
        return self._conn
    
    def _get_credentials(self):
        client = boto3.client("secretsmanager", region_name="us-east-1")
        secret = client.get_secret_value(SecretId="prod/redshift/analytics")
        return json.loads(secret["SecretString"])
    
    def load_full(self, s3_path: str, target_table: str, iam_role: str):
        """Full refresh: TRUNCATE + COPY."""
        cursor = self.conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {target_table}")
        cursor.execute(f"""
            COPY {target_table}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            FORMAT PARQUET
        """)
        self.conn.commit()
        logger.info(f"Full load to {target_table} from {s3_path}")
    
    def load_upsert(self, s3_path: str, target_table: str, 
                   key_columns: List[str], iam_role: str):
        """Upsert: Load to staging, MERGE into target."""
        staging = f"{target_table}_staging"
        key_condition = " AND ".join(
            f"target.{k} = staging.{k}" for k in key_columns
        )
        
        cursor = self.conn.cursor()
        
        # Load to staging
        cursor.execute(f"CREATE TEMP TABLE {staging} (LIKE {target_table})")
        cursor.execute(f"""
            COPY {staging}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            FORMAT PARQUET
        """)
        
        # Delete matching keys
        cursor.execute(f"""
            DELETE FROM {target_table} 
            USING {staging}
            WHERE {key_condition}
        """)
        
        # Insert all from staging
        cursor.execute(f"INSERT INTO {target_table} SELECT * FROM {staging}")
        cursor.execute(f"DROP TABLE {staging}")
        
        self.conn.commit()
        logger.info(f"Upsert to {target_table} complete")
