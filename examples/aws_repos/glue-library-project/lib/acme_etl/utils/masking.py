"""
PII masking utilities — used before writing to non-privileged zones.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def mask_pii(df: DataFrame, pii_config: list) -> DataFrame:
    """
    Mask PII columns based on configuration.
    
    pii_config format:
    [
        {"column": "ssn", "strategy": "hash"},
        {"column": "email", "strategy": "partial", "show_chars": 3},
        {"column": "phone", "strategy": "redact"},
        {"column": "name", "strategy": "tokenize"},
        {"column": "dob", "strategy": "year_only"},
    ]
    """
    for config in pii_config:
        col = config["column"]
        strategy = config["strategy"]
        
        if strategy == "hash":
            df = df.withColumn(col, F.sha2(F.col(col).cast("string"), 256))
        
        elif strategy == "partial":
            show = config.get("show_chars", 3)
            df = df.withColumn(col, 
                F.concat(F.substring(F.col(col), 1, show), F.lit("***"))
            )
        
        elif strategy == "redact":
            df = df.withColumn(col, F.lit("[REDACTED]"))
        
        elif strategy == "tokenize":
            # Deterministic tokenization (same input = same token)
            df = df.withColumn(col, 
                F.concat(F.lit("TOK_"), F.substring(F.md5(F.col(col).cast("string")), 1, 12))
            )
        
        elif strategy == "year_only":
            df = df.withColumn(col, F.year(F.col(col)))
    
    return df


def tokenize_field(df: DataFrame, column: str, salt: str = "acme_2024") -> DataFrame:
    """Deterministic tokenization with salt."""
    return df.withColumn(column, 
        F.sha2(F.concat(F.col(column).cast("string"), F.lit(salt)), 256)
    )


def hash_field(df: DataFrame, column: str) -> DataFrame:
    """Simple SHA-256 hash."""
    return df.withColumn(f"{column}_hash", F.sha2(F.col(column).cast("string"), 256))
