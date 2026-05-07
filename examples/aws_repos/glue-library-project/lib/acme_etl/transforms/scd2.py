"""
SCD Type 2 implementation for slowly-changing dimensions.
Used by: customer_dim, product_dim, account_dim jobs.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from datetime import datetime


def detect_changes(source_df: DataFrame, target_df: DataFrame, 
                   key_cols: list, compare_cols: list) -> dict:
    """Detect inserts, updates, and unchanged records between source and target."""
    
    # Join source to current target
    join_cond = [source_df[k] == target_df[k] for k in key_cols]
    
    joined = source_df.alias("src").join(
        target_df.filter(F.col("is_current") == True).alias("tgt"),
        join_cond,
        "full_outer"
    )
    
    # New records (in source but not target)
    new_records = joined.filter(
        F.col("tgt.{0}".format(key_cols[0])).isNull()
    ).select("src.*")
    
    # Build change detection expression
    change_expr = F.lit(False)
    for col in compare_cols:
        change_expr = change_expr | (
            F.coalesce(F.col(f"src.{col}"), F.lit("__NULL__")) != 
            F.coalesce(F.col(f"tgt.{col}"), F.lit("__NULL__"))
        )
    
    # Changed records
    changed = joined.filter(
        F.col("src.{0}".format(key_cols[0])).isNotNull() &
        F.col("tgt.{0}".format(key_cols[0])).isNotNull() &
        change_expr
    ).select("src.*")
    
    # Unchanged
    unchanged = joined.filter(
        F.col("src.{0}".format(key_cols[0])).isNotNull() &
        F.col("tgt.{0}".format(key_cols[0])).isNotNull() &
        ~change_expr
    ).select("tgt.*")
    
    return {"new": new_records, "changed": changed, "unchanged": unchanged}


def scd2_merge(source_df: DataFrame, target_table: str,
               key_cols: list, compare_cols: list,
               spark_session=None) -> dict:
    """
    Perform full SCD2 merge: close old records, insert new versions.
    
    Args:
        source_df: Incoming data
        target_table: S3 path or Glue catalog table
        key_cols: Business key columns
        compare_cols: Columns to detect changes on
    
    Returns:
        dict with counts: {inserted, updated, unchanged}
    """
    from pyspark.sql import SparkSession
    spark = spark_session or SparkSession.builder.getOrCreate()
    
    now = datetime.utcnow()
    
    # Read current target
    try:
        target_df = spark.read.parquet(target_table)
    except:
        # First run — no target exists
        result_df = (
            source_df
            .withColumn("effective_from", F.lit(now))
            .withColumn("effective_to", F.lit(datetime(9999, 12, 31)))
            .withColumn("is_current", F.lit(True))
            .withColumn("_version", F.lit(1))
            .withColumn("_checksum", F.md5(F.concat_ws("||", *compare_cols)))
        )
        result_df.write.mode("overwrite").parquet(target_table)
        return {"inserted": result_df.count(), "updated": 0, "unchanged": 0}
    
    changes = detect_changes(source_df, target_df, key_cols, compare_cols)
    
    # Close old versions of changed records
    closed_records = (
        target_df
        .join(changes["changed"].select(key_cols), key_cols, "inner")
        .filter(F.col("is_current") == True)
        .withColumn("effective_to", F.lit(now))
        .withColumn("is_current", F.lit(False))
    )
    
    # Unchanged current records stay as-is
    unchanged_current = (
        target_df
        .join(changes["changed"].select(key_cols), key_cols, "left_anti")
    )
    
    # New versions of changed records + brand new records
    max_version = target_df.agg(F.max("_version")).collect()[0][0] or 0
    
    new_versions = (
        changes["changed"].unionByName(changes["new"], allowMissingColumns=True)
        .withColumn("effective_from", F.lit(now))
        .withColumn("effective_to", F.lit(datetime(9999, 12, 31)))
        .withColumn("is_current", F.lit(True))
        .withColumn("_version", F.lit(max_version + 1))
        .withColumn("_checksum", F.md5(F.concat_ws("||", *compare_cols)))
    )
    
    # Write merged result
    final = unchanged_current.unionByName(closed_records).unionByName(new_versions, allowMissingColumns=True)
    final.write.mode("overwrite").parquet(target_table)
    
    return {
        "inserted": changes["new"].count(),
        "updated": changes["changed"].count(),
        "unchanged": changes["unchanged"].count()
    }
