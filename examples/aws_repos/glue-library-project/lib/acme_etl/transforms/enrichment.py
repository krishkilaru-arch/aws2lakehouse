"""
Enrichment patterns: lookups, broadcast joins, reference data.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def lookup_join(fact_df: DataFrame, dim_df: DataFrame,
                join_key: str, select_cols: list = None,
                default_values: dict = None) -> DataFrame:
    """Left join with dimension table, fill defaults for unmatched."""
    if select_cols:
        dim_subset = dim_df.select(join_key, *select_cols)
    else:
        dim_subset = dim_df
    
    result = fact_df.join(dim_subset, join_key, "left")
    
    if default_values:
        for col, default in default_values.items():
            result = result.withColumn(col, F.coalesce(F.col(col), F.lit(default)))
    
    return result


def broadcast_enrich(df: DataFrame, ref_table_path: str,
                     join_key: str, spark_session=None) -> DataFrame:
    """Load small reference table and broadcast join."""
    from pyspark.sql import SparkSession
    spark = spark_session or SparkSession.builder.getOrCreate()
    
    ref_df = spark.read.parquet(ref_table_path)
    return df.join(F.broadcast(ref_df), join_key, "left")


def geo_enrich(df: DataFrame, lat_col: str, lon_col: str,
               geo_ref_path: str) -> DataFrame:
    """Enrich with geographic region based on lat/lon."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    geo_ref = spark.read.parquet(geo_ref_path)
    
    # Simple bounding box lookup (production would use H3 or spatial index)
    return df.crossJoin(F.broadcast(geo_ref)).filter(
        (F.col(lat_col).between(F.col("min_lat"), F.col("max_lat"))) &
        (F.col(lon_col).between(F.col("min_lon"), F.col("max_lon")))
    )
