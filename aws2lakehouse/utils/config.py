"""Configuration management for aws2lakehouse."""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class Config:
    """Centralized configuration management."""
    
    DEFAULT_CONFIG = {
        "target_catalog": "production",
        "environments": {
            "dev": {"catalog": "dev", "schema_prefix": "dev_"},
            "staging": {"catalog": "staging", "schema_prefix": "stg_"},
            "production": {"catalog": "production", "schema_prefix": ""},
        },
        "compute": {
            "default_spark_version": "15.4.x-scala2.12",
            "default_node_type": "i3.xlarge",
            "serverless_enabled": True,
        },
        "governance": {
            "naming_convention": "{catalog}.{domain}_{layer}.{table_name}",
            "enable_mnpi_controls": True,
            "enable_row_level_security": False,
        },
        "migration": {
            "max_parallel_jobs": 10,
            "retry_count": 3,
            "timeout_minutes": 120,
            "backup_before_migrate": True,
        },
    }
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = self.DEFAULT_CONFIG.copy()
        if config_path and os.path.exists(config_path):
            with open(config_path, "r") as f:
                user_config = yaml.safe_load(f)
                self._deep_merge(self.config, user_config)
    
    def get(self, key: str, default=None) -> Any:
        """Get config value using dot notation (e.g., 'compute.default_spark_version')."""
        keys = key.split(".")
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            if value is None:
                return default
        return value
    
    def _deep_merge(self, base: Dict, override: Dict):
        """Deep merge override into base dict."""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
