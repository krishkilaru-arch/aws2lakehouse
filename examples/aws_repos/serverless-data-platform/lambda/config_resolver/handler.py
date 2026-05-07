"""
config_resolver — Resolves pipeline configuration from DynamoDB.
Handles environment-specific overrides, feature flags, and dynamic routing.
"""
import json
import boto3
import os
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
config_table = dynamodb.Table(os.environ.get("CONFIG_TABLE", "pipeline_config"))
ssm = boto3.client("ssm")


def handler(event, context):
    """Resolve full pipeline configuration for execution."""
    
    source_type = event["source_type"]
    source_name = event["source_name"]
    environment = os.environ.get("ENVIRONMENT", "production")
    
    # Get base config
    config = get_config(source_type, source_name)
    
    # Apply environment overrides
    env_overrides = config.get("env_overrides", {}).get(environment, {})
    config.update(env_overrides)
    
    # Resolve secrets references
    if "secret_refs" in config:
        for key, ssm_path in config["secret_refs"].items():
            config[key] = resolve_secret(ssm_path)
    
    # Check feature flags
    config["features"] = {
        "enable_dq_gate": config.get("enable_dq_gate", True),
        "enable_gold_layer": config.get("enable_gold_layer", True),
        "enable_notifications": config.get("enable_notifications", True),
        "dry_run": config.get("dry_run", False),
    }
    
    return {
        **event,
        "resolved_config": config,
        "environment": environment,
        "resolved_at": datetime.utcnow().isoformat(),
    }


def get_config(source_type, source_name):
    response = config_table.get_item(Key={"source_type": source_type, "source_name": source_name})
    return response.get("Item", {})


def resolve_secret(ssm_path):
    try:
        response = ssm.get_parameter(Name=ssm_path, WithDecryption=True)
        return response["Parameter"]["Value"]
    except:
        return None
