"""
Data Quality Validators — reusable validation rules for financial data.

Pattern: Each validator returns a DataFrame with a quality_result column.
Rules are composable and configurable per pipeline.
"""
from pyspark.sql import DataFrame, functions as F
from typing import List, Dict, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation check."""
    rule_name: str
    passed: bool
    total_records: int
    failed_records: int
    failure_pct: float
    details: str


class DataValidator:
    """Composable data quality validator for financial data."""
    
    def __init__(self, df: DataFrame, pipeline_name: str):
        self.df = df
        self.pipeline_name = pipeline_name
        self.results: List[ValidationResult] = []
    
    def check_not_null(self, columns: List[str], threshold: float = 0.0) -> "DataValidator":
        """Validate columns are not null (with optional threshold)."""
        total = self.df.count()
        for col in columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            failure_pct = null_count / total if total > 0 else 0
            self.results.append(ValidationResult(
                rule_name=f"not_null_{col}",
                passed=failure_pct <= threshold,
                total_records=total,
                failed_records=null_count,
                failure_pct=failure_pct,
                details=f"Column {col}: {null_count} nulls ({failure_pct:.2%})"
            ))
        return self
    
    def check_unique(self, columns: List[str]) -> "DataValidator":
        """Validate uniqueness of column or composite key."""
        total = self.df.count()
        distinct = self.df.select(columns).distinct().count()
        duplicates = total - distinct
        self.results.append(ValidationResult(
            rule_name=f"unique_{'_'.join(columns)}",
            passed=duplicates == 0,
            total_records=total,
            failed_records=duplicates,
            failure_pct=duplicates / total if total > 0 else 0,
            details=f"Columns {columns}: {duplicates} duplicate rows"
        ))
        return self
    
    def check_range(self, column: str, min_val: float = None, max_val: float = None) -> "DataValidator":
        """Validate numeric values are within expected range."""
        total = self.df.count()
        condition = F.lit(False)
        if min_val is not None:
            condition = condition | (F.col(column) < min_val)
        if max_val is not None:
            condition = condition | (F.col(column) > max_val)
        
        failed = self.df.filter(condition).count()
        self.results.append(ValidationResult(
            rule_name=f"range_{column}",
            passed=failed == 0,
            total_records=total,
            failed_records=failed,
            failure_pct=failed / total if total > 0 else 0,
            details=f"Column {column}: {failed} out of range [{min_val}, {max_val}]"
        ))
        return self
    
    def check_referential_integrity(self, column: str, reference_df: DataFrame, 
                                      ref_column: str) -> "DataValidator":
        """Validate foreign key references exist in reference table."""
        total = self.df.count()
        orphans = (
            self.df.select(column)
            .join(reference_df.select(ref_column), 
                  F.col(column) == F.col(ref_column), "left_anti")
            .count()
        )
        self.results.append(ValidationResult(
            rule_name=f"ref_integrity_{column}",
            passed=orphans == 0,
            total_records=total,
            failed_records=orphans,
            failure_pct=orphans / total if total > 0 else 0,
            details=f"Column {column}: {orphans} orphan records"
        ))
        return self
    
    def validate(self, fail_on_error: bool = True) -> List[ValidationResult]:
        """Execute all checks and optionally fail the job."""
        failures = [r for r in self.results if not r.passed]
        
        for r in self.results:
            status = "✅ PASS" if r.passed else "❌ FAIL"
            logger.info(f"[{self.pipeline_name}] {status} {r.rule_name}: {r.details}")
        
        if failures and fail_on_error:
            msg = f"Data quality check failed: {len(failures)} rules violated"
            logger.error(msg)
            raise ValueError(msg)
        
        return self.results
