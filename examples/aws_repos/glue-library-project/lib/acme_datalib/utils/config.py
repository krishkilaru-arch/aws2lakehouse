"""
Configuration management — load from S3 YAML or SSM Parameter Store.
"""
import boto3
import json
import yaml
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class PipelineConfig:
    """Load and manage pipeline configuration from AWS sources."""
    
    def __init__(self, environment: str = "prod"):
        self.environment = environment
        self._config_cache: Dict[str, Any] = {}
    
    @classmethod
    def from_s3(cls, bucket: str, key: str, environment: str = "prod") -> "PipelineConfig":
        """Load config from S3 YAML file."""
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        
        instance = cls(environment)
        full_config = yaml.safe_load(content)
        instance._config_cache = full_config.get(environment, full_config)
        return instance
    
    @classmethod
    def from_ssm(cls, prefix: str, environment: str = "prod") -> "PipelineConfig":
        """Load config from SSM Parameter Store."""
        ssm = boto3.client("ssm")
        paginator = ssm.get_paginator("get_parameters_by_path")
        
        instance = cls(environment)
        path = f"/{environment}/{prefix}"
        
        for page in paginator.paginate(Path=path, Recursive=True, WithDecryption=True):
            for param in page["Parameters"]:
                key = param["Name"].replace(path + "/", "")
                instance._config_cache[key] = param["Value"]
        
        return instance
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a config value."""
        return self._config_cache.get(key, default)
    
    def get_connection(self, name: str) -> Dict[str, str]:
        """Get database connection config."""
        return {
            "host": self.get(f"connections/{name}/host"),
            "port": self.get(f"connections/{name}/port"),
            "database": self.get(f"connections/{name}/database"),
            "secret_name": self.get(f"connections/{name}/secret_name"),
        }
