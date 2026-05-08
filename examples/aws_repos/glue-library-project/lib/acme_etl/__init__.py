"""
acme_etl — Enterprise ETL framework.

Usage in Glue jobs:
    from acme_etl.transforms import scd2_merge, dedup_by_key, flatten_nested
    from acme_etl.quality import run_quality_checks, validate_schema
    from acme_etl.io import read_source, write_target
    from acme_etl.utils import mask_pii, generate_surrogate_key, audit_log
"""
__version__ = "3.2.1"
