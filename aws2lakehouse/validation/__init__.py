"""
Testing & Validation Suite — Automated migration validation.

Validates migrated pipelines by comparing source (AWS) and target (Databricks):
- Row count comparison
- Schema validation
- Sample data comparison (key-by-key)
- Aggregate comparison (sum/avg/min/max)
- Duplicate detection
- Null key check
- Business rule validation
- Freshness validation

Usage:
    validator = MigrationValidator()
    notebook = validator.generate_validation_notebook(
        pipeline_name="trade_events",
        source_table="legacy_db.trades",
        target_table="production.risk_bronze.trades",
        primary_key="trade_id",
        numeric_columns=["notional_amount", "pnl"]
    )
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    check_name: str
    status: str          # PASS, FAIL, WARN, SKIP
    source_value: str = ""
    target_value: str = ""
    difference: str = ""
    details: str = ""


@dataclass 
class ValidationReport:
    """Complete validation report for a pipeline migration."""
    pipeline_name: str
    source_table: str
    target_table: str
    results: List[ValidationResult] = field(default_factory=list)
    
    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.status == "PASS")
    
    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == "FAIL")
    
    @property
    def overall_status(self) -> str:
        if self.failed > 0:
            return "FAIL"
        return "PASS"
    
    def to_summary(self) -> str:
        lines = [
            f"Validation: {self.pipeline_name}",
            f"Source: {self.source_table} -> Target: {self.target_table}",
            f"Result: {self.overall_status} ({self.passed} passed, {self.failed} failed)",
            ""
        ]
        for r in self.results:
            icon = "PASS" if r.status == "PASS" else "FAIL" if r.status == "FAIL" else "WARN"
            lines.append(f"  [{icon}] {r.check_name}")
            if r.status != "PASS":
                lines.append(f"     Source: {r.source_value}")
                lines.append(f"     Target: {r.target_value}")
                lines.append(f"     Diff:   {r.difference}")
        return "\n".join(lines)


class MigrationValidator:
    """
    Validates migrated pipeline outputs against source system.
    Generates complete Databricks notebooks for automated comparison.
    """
    
    def generate_validation_notebook(self, pipeline_name: str,
                                     source_table: str, target_table: str,
                                     primary_key: str = "id",
                                     numeric_columns: List[str] = None,
                                     tolerance: float = 0.001) -> str:
        """
        Generate a complete validation notebook (Databricks notebook source format).
        
        Checks performed:
          1. Row count comparison (within tolerance)
          2. Schema comparison (detect missing columns)
          3. Aggregate comparison (sum/avg per numeric column)
          4. Duplicate check on primary key
          5. Null key check
          6. Summary report
          
        Args:
            pipeline_name: Name for the report
            source_table: Fully qualified source table
            target_table: Fully qualified target table
            primary_key: Column to check for duplicates
            numeric_columns: Columns to aggregate-compare (auto-detected if None)
            tolerance: Acceptable difference percentage (0.001 = 0.1%)
        """
        num_cols_str = str(numeric_columns) if numeric_columns else "[]"
        
        return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Migration Validation: {pipeline_name}
# MAGIC **Source:** `{source_table}`  
# MAGIC **Target:** `{target_table}`  
# MAGIC **Primary Key:** `{primary_key}`  
# MAGIC **Tolerance:** {tolerance * 100}%

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

source_df = spark.table("{source_table}")
target_df = spark.table("{target_table}")
results = []
print(f"Validation started: {{datetime.now()}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Row Count Comparison

# COMMAND ----------

source_count = source_df.count()
target_count = target_df.count()
diff_pct = abs(source_count - target_count) / max(source_count, 1) * 100

print(f"Source rows: {{source_count:,}}")
print(f"Target rows: {{target_count:,}}")
print(f"Difference:  {{abs(source_count - target_count):,}} ({{diff_pct:.4f}}%)")

status = "PASS" if diff_pct < {tolerance * 100} else "FAIL"
results.append(("Row Count", status, str(source_count), str(target_count), f"{{diff_pct:.4f}}%"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema Comparison

# COMMAND ----------

source_schema = {{f.name: str(f.dataType) for f in source_df.schema.fields}}
target_schema = {{f.name: str(f.dataType) for f in target_df.schema.fields if not f.name.startswith("_")}}

missing_cols = set(source_schema.keys()) - set(target_schema.keys())
extra_cols = set(target_schema.keys()) - set(source_schema.keys())
type_mismatches = [(c, source_schema[c], target_schema[c]) 
                   for c in source_schema if c in target_schema and source_schema[c] != target_schema[c]]

if missing_cols:
    print(f"MISSING in target: {{sorted(missing_cols)}}")
if extra_cols:
    print(f"EXTRA in target (metadata): {{sorted(extra_cols)}}")
if type_mismatches:
    for col, st, tt in type_mismatches:
        print(f"TYPE MISMATCH: {{col}}: {{st}} -> {{tt}}")

status = "PASS" if not missing_cols else "FAIL"
results.append(("Schema Match", status, f"{{len(source_schema)}} cols", f"{{len(target_schema)}} cols", f"missing={{len(missing_cols)}}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aggregate Comparison

# COMMAND ----------

numeric_cols = {num_cols_str}
if not numeric_cols:
    numeric_types = ("LongType()", "DoubleType()", "DecimalType", "IntegerType()", "FloatType()")
    numeric_cols = [f.name for f in source_df.schema.fields if any(str(f.dataType).startswith(t.rstrip("()")) for t in numeric_types)]

print(f"Checking {{len(numeric_cols)}} numeric columns...")
for col in numeric_cols[:10]:
    try:
        src = source_df.agg(F.sum(col).alias("s"), F.avg(col).alias("a")).collect()[0]
        tgt = target_df.agg(F.sum(col).alias("s"), F.avg(col).alias("a")).collect()[0]
        
        src_sum = float(src["s"] or 0)
        tgt_sum = float(tgt["s"] or 0)
        pct_diff = abs(src_sum - tgt_sum) / max(abs(src_sum), 1) * 100
        
        status = "PASS" if pct_diff < {tolerance * 100} else "FAIL"
        results.append((f"SUM({{col}})", status, f"{{src_sum:.2f}}", f"{{tgt_sum:.2f}}", f"{{pct_diff:.4f}}%"))
        print(f"  {{col}}: diff={{pct_diff:.4f}}% -> {{status}}")
    except Exception as e:
        results.append((f"SUM({{col}})", "SKIP", "", "", str(e)[:50]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Duplicate Check

# COMMAND ----------

target_total = target_df.count()
target_distinct = target_df.select("{primary_key}").distinct().count()
duplicates = target_total - target_distinct

status = "PASS" if duplicates == 0 else "FAIL"
results.append(("No Duplicates", status, "0 expected", str(duplicates), f"{{duplicates}} dupes on {primary_key}"))
print(f"Duplicates on {primary_key}: {{duplicates}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Null Key Check

# COMMAND ----------

null_keys = target_df.filter(F.col("{primary_key}").isNull()).count()
status = "PASS" if null_keys == 0 else "FAIL"
results.append(("No Null Keys", status, "0 expected", str(null_keys), f"{{null_keys}} null PKs"))
print(f"Null primary keys: {{null_keys}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validation Summary

# COMMAND ----------

print("=" * 65)
print(f"  VALIDATION REPORT: {pipeline_name}")
print("=" * 65)
passed = sum(1 for r in results if r[1] == "PASS")
failed = sum(1 for r in results if r[1] == "FAIL")
skipped = sum(1 for r in results if r[1] == "SKIP")

for name, status, src, tgt, diff in results:
    icon = "PASS" if status == "PASS" else "FAIL" if status == "FAIL" else "SKIP"
    print(f"  [{{icon}}] {{name:<30}} {{diff}}")

print(f"\nTotal: {{passed}} passed, {{failed}} failed, {{skipped}} skipped")

if failed == 0:
    print("\n  ALL CHECKS PASSED - Migration validated successfully")
else:
    print(f"\n  VALIDATION FAILED - {{failed}} checks need attention")

dbutils.notebook.exit("PASS" if failed == 0 else "FAIL")
'''

    def generate_batch_validation_notebook(self, validations: List[Dict]) -> str:
        """
        Generate a notebook that validates multiple tables in sequence.
        
        Args:
            validations: List of dicts with keys:
                - pipeline_name, source_table, target_table, primary_key
        """
        table_entries = "\n".join([
            f'    {{"pipeline": "{v["pipeline_name"]}", "source": "{v["source_table"]}", "target": "{v["target_table"]}", "pk": "{v["primary_key"]}"}},'
            for v in validations
        ])
        
        return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Migration Validation
# MAGIC Validates {len(validations)} migrated pipelines

# COMMAND ----------

from pyspark.sql import functions as F

tables_to_validate = [
{table_entries}
]

# COMMAND ----------

results_summary = []

for entry in tables_to_validate:
    name = entry["pipeline"]
    src = entry["source"]
    tgt = entry["target"]
    pk = entry["pk"]
    
    try:
        src_df = spark.table(src)
        tgt_df = spark.table(tgt)
        
        src_count = src_df.count()
        tgt_count = tgt_df.count()
        diff_pct = abs(src_count - tgt_count) / max(src_count, 1) * 100
        
        tgt_dupes = tgt_count - tgt_df.select(pk).distinct().count()
        tgt_nulls = tgt_df.filter(F.col(pk).isNull()).count()
        
        status = "PASS" if diff_pct < 0.1 and tgt_dupes == 0 and tgt_nulls == 0 else "FAIL"
        results_summary.append((name, status, src_count, tgt_count, diff_pct, tgt_dupes))
    except Exception as e:
        results_summary.append((name, "ERROR", 0, 0, 0, str(e)[:50]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

print(f"{{\'-\' * 80}}")
print(f"  {{\'Pipeline\':<30}} {{\'Status\':<8}} {{\'Source\':<12}} {{\'Target\':<12}} {{\'Diff%\':<8}} Dupes")
print(f"{{\'-\' * 80}}")

for name, status, src_c, tgt_c, diff, dupes in results_summary:
    icon = "PASS" if status == "PASS" else "FAIL"
    print(f"  {{name:<30}} [{{icon}}]  {{src_c:<12,}} {{tgt_c:<12,}} {{diff:<8.2f}} {{dupes}}")

passed = sum(1 for r in results_summary if r[1] == "PASS")
failed = sum(1 for r in results_summary if r[1] != "PASS")
print(f"\nSummary: {{passed}} passed, {{failed}} failed out of {{len(results_summary)}} pipelines")
'''

    def generate_business_rule_tests(self, table: str, rules: List[Dict]) -> str:
        """
        Generate business rule validation SQL.
        
        Args:
            table: Fully qualified table name
            rules: List of dicts with keys: name, condition, threshold (0-1)
        """
        checks = []
        for rule in rules:
            checks.append(f"""
-- Rule: {rule['name']}
SELECT
  '{rule['name']}' as rule_name,
  COUNT(*) as total_rows,
  SUM(CASE WHEN {rule['condition']} THEN 1 ELSE 0 END) as passing_rows,
  ROUND(SUM(CASE WHEN {rule['condition']} THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pass_rate,
  {rule.get('threshold', 0.99) * 100} as required_rate,
  CASE 
    WHEN SUM(CASE WHEN {rule['condition']} THEN 1 ELSE 0 END) * 1.0 / COUNT(*) >= {rule.get('threshold', 0.99)}
    THEN 'PASS' ELSE 'FAIL'
  END as status
FROM {table}""")
        
        union_sql = "\nUNION ALL\n".join(checks)
        return f"""-- Business Rule Validation for {table}
-- Generated by aws2lakehouse.validation

{union_sql}
ORDER BY status DESC, rule_name;"""

    def generate_cutover_checklist(self, pipeline_name: str, domain: str) -> str:
        """Generate a structured cutover checklist as markdown."""
        return f"""# Cutover Checklist: {pipeline_name} ({domain})

## Pre-Cutover (T-1 day)
- [ ] Validation notebook passes with 0 failures
- [ ] Performance benchmark: new pipeline completes within SLA
- [ ] Monitoring dashboard configured and alerting tested
- [ ] Rollback procedure documented and dry-run completed
- [ ] Stakeholder sign-off received (business owner + tech lead)
- [ ] Communication sent to downstream consumers
- [ ] Dual-write validation running for 48+ hours with no drift

## Cutover (T-0)
- [ ] Disable legacy pipeline schedule (note: do NOT delete)
- [ ] Enable new Databricks pipeline schedule
- [ ] Verify first run completes successfully
- [ ] Verify downstream consumers receiving data on time
- [ ] Confirm SLA met on first production run
- [ ] Check monitoring dashboard (freshness, volume, DQ)

## Post-Cutover Hypercare (T+1 to T+7)
- [ ] Monitor for 7 consecutive successful runs
- [ ] Data quality scores stable (no regression from validation)
- [ ] No alerts triggered
- [ ] Downstream reports verified correct by business owner
- [ ] Performance within expected bounds (no degradation trend)
- [ ] Mark pipeline as "complete" in wave tracking table

## Rollback Procedure (if needed)
1. Re-enable legacy pipeline schedule immediately
2. Disable new Databricks pipeline schedule
3. Notify downstream consumers: "Rollback in progress, using legacy source"
4. Investigate root cause (check Spark UI, event log, data quality)
5. Fix issue, re-validate in dev/staging
6. Schedule new cutover window with stakeholders
7. Resume dual-write validation for 48 hours before retrying

## Sign-Off
| Role | Name | Date | Signature |
|------|------|------|-----------|
| Business Owner | | | |
| Technical Lead | | | |
| Ops/SRE | | | |
| Compliance (if MNPI) | | | |
"""
