"""
Surrogate key generation utilities.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def generate_surrogate_key(df: DataFrame, key_col: str = "sk_id") -> DataFrame:
    """Generate monotonically increasing surrogate key."""
    return df.withColumn(key_col, F.monotonically_increasing_id())


def generate_hash_key(df: DataFrame, source_cols: list, 
                      key_col: str = "hash_key") -> DataFrame:
    """Generate deterministic hash key from source columns."""
    return df.withColumn(key_col, F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in source_cols])))
