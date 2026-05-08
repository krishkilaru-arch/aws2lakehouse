"""
Deduplication patterns used across multiple pipelines.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dedup_by_key(df: DataFrame, key_cols: list, order_col: str, 
                 ascending: bool = False) -> DataFrame:
    """Keep latest record per key (based on order column)."""
    window = Window.partitionBy(*key_cols).orderBy(
        F.col(order_col).asc() if ascending else F.col(order_col).desc()
    )
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def dedup_by_window(df: DataFrame, key_cols: list, 
                    timestamp_col: str, window_minutes: int = 5) -> DataFrame:
    """Deduplicate within a time window (for late-arriving duplicates)."""
    window = Window.partitionBy(*key_cols).orderBy(F.col(timestamp_col).desc())
    
    return (
        df
        .withColumn("_prev_ts", F.lag(timestamp_col).over(window))
        .withColumn("_is_dup", 
            (F.col(timestamp_col).cast("long") - F.col("_prev_ts").cast("long")) < (window_minutes * 60)
        )
        .filter(F.col("_is_dup").isNull() | (F.col("_is_dup") == False))
        .drop("_prev_ts", "_is_dup")
    )


def dedup_with_merge_strategy(df: DataFrame, key_cols: list,
                              merge_cols: dict) -> DataFrame:
    """
    Merge duplicate records instead of just keeping latest.
    merge_cols: {"col_name": "strategy"} where strategy is "latest", "first", "max", "concat"
    """
    agg_exprs = []
    for col, strategy in merge_cols.items():
        if strategy == "latest":
            agg_exprs.append(F.last(col, ignorenulls=True).alias(col))
        elif strategy == "first":
            agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
        elif strategy == "max":
            agg_exprs.append(F.max(col).alias(col))
        elif strategy == "min":
            agg_exprs.append(F.min(col).alias(col))
        elif strategy == "concat":
            agg_exprs.append(F.concat_ws(", ", F.collect_set(col)).alias(col))
        elif strategy == "sum":
            agg_exprs.append(F.sum(col).alias(col))
    
    return df.groupBy(*key_cols).agg(*agg_exprs)
