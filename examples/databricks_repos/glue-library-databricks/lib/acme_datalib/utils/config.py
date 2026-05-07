"""
Configuration — Databricks Edition.

Changes: S3 YAML / SSM → Databricks widgets + secret scopes
"""
from typing import Any


class PipelineConfig:
    """Load config from Databricks widgets and secret scopes."""
    
    def __init__(self, environment: str = "prod"):
        self.environment = environment
        self._config = {}
    
    @classmethod
    def from_widgets(cls, environment: str = None) -> "PipelineConfig":
        """Load config from notebook widgets."""
        env = environment or dbutils.widgets.get("environment")
        instance = cls(env)
        instance._config = {
            "catalog": f"production" if env == "prod" else f"{env}_catalog",
            "volume_base": f"/Volumes/production/raw",
        }
        return instance
    
    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)
