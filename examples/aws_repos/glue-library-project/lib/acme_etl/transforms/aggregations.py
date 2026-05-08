"""
Reusable aggregation patterns.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def running_total(df: DataFrame, partition_col: str, 
                  order_col: str, value_col: str) -> DataFrame:
    """Calculate running total partitioned by key."""
    window = Window.partitionBy(partition_col).orderBy(order_col).rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    return df.withColumn(f"{value_col}_running_total", F.sum(value_col).over(window))


def period_summary(df: DataFrame, date_col: str, value_cols: list,
                   period: str = "month") -> DataFrame:
    """Aggregate by time period (day/week/month/quarter/year)."""
    if period == "day":
        group_col = F.date_trunc("day", F.col(date_col))
    elif period == "week":
        group_col = F.date_trunc("week", F.col(date_col))
    elif period == "month":
        group_col = F.date_trunc("month", F.col(date_col))
    elif period == "quarter":
        group_col = F.date_trunc("quarter", F.col(date_col))
    else:
        group_col = F.date_trunc("year", F.col(date_col))
    
    df_with_period = df.withColumn("_period", group_col)
    
    agg_exprs = []
    for col in value_cols:
        agg_exprs.extend([
            F.sum(col).alias(f"{col}_sum"),
            F.avg(col).alias(f"{col}_avg"),
            F.min(col).alias(f"{col}_min"),
            F.max(col).alias(f"{col}_max"),
            F.count(col).alias(f"{col}_count"),
        ])
    
    return df_with_period.groupBy("_period").agg(*agg_exprs).orderBy("_period")
