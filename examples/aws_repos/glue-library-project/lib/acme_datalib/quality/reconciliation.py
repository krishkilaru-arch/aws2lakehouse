"""
Reconciliation Framework — compare datasets for completeness and accuracy.

Used for: Internal vs Custodian, Source vs Target, T-1 vs T comparisons.
"""
from pyspark.sql import DataFrame, functions as F
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)


class ReconciliationEngine:
    """Compare two datasets and report differences."""
    
    def __init__(self, source_name: str, target_name: str):
        self.source_name = source_name
        self.target_name = target_name
    
    def count_reconciliation(self, source: DataFrame, target: DataFrame) -> dict:
        """Simple record count comparison."""
        s_count = source.count()
        t_count = target.count()
        return {
            "source_count": s_count,
            "target_count": t_count,
            "difference": abs(s_count - t_count),
            "match": s_count == t_count
        }
    
    def value_reconciliation(self, source: DataFrame, target: DataFrame,
                            key_columns: List[str], value_columns: List[str],
                            tolerance: float = 0.01) -> DataFrame:
        """Compare values between source and target with tolerance."""
        joined = source.alias("src").join(
            target.alias("tgt"), key_columns, "full_outer"
        )
        
        for col in value_columns:
            joined = joined.withColumn(
                f"{col}_diff",
                F.abs(F.coalesce(F.col(f"src.{col}"), F.lit(0)) - 
                      F.coalesce(F.col(f"tgt.{col}"), F.lit(0)))
            ).withColumn(
                f"{col}_break",
                F.col(f"{col}_diff") > tolerance
            )
        
        return joined
