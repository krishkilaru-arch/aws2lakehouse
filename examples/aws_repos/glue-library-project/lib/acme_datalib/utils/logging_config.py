"""
Structured logging for Glue jobs — JSON format for CloudWatch Insights.
"""
import logging
import json
import sys
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """Format log records as JSON for CloudWatch Insights parsing."""
    
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "job_name": getattr(record, "job_name", "unknown"),
            "run_id": getattr(record, "run_id", "unknown"),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


def setup_logging(job_name: str, run_id: str = "", level: str = "INFO"):
    """Configure structured logging for a Glue job."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    
    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(getattr(logging, level))
    
    # Add context to all log records
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.job_name = job_name
        record.run_id = run_id
        return record
    logging.setLogRecordFactory(record_factory)
    
    return logging.getLogger(job_name)
