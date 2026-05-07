"""Complexity Analyzer — Classifies pipelines by migration difficulty."""

from typing import Dict, List, Any
from dataclasses import dataclass, field
import re
import logging

logger = logging.getLogger(__name__)


@dataclass
class ComplexityScore:
    overall_score: float
    category: str
    code_complexity: float = 0.0
    data_complexity: float = 0.0
    dependency_complexity: float = 0.0
    operational_complexity: float = 0.0
    has_custom_jars: bool = False
    has_udfs: bool = False
    has_streaming: bool = False
    has_ml_models: bool = False
    has_external_apis: bool = False
    uses_hive_metastore: bool = False
    estimated_hours: float = 0.0
    recommended_approach: str = ""
    findings: List[str] = field(default_factory=list)
    blockers: List[str] = field(default_factory=list)


class ComplexityAnalyzer:
    """Analyzes pipeline code to determine migration complexity."""

    EFFORT_MAP = {"simple": 4, "medium": 16, "complex": 40, "critical": 80}

    def __init__(self):
        self.results: List[ComplexityScore] = []

    def analyze_code(self, source_code: str, metadata: Dict = None) -> ComplexityScore:
        """Analyze source code and return complexity score."""
        metadata = metadata or {}
        findings = []
        lines = source_code.splitlines()
        loc = len([l for l in lines if l.strip() and not l.strip().startswith("#")])

        # Code complexity
        code_score = 0.0
        if loc > 500: code_score += 30; findings.append(f"Large codebase: {loc} lines")
        elif loc > 200: code_score += 15
        elif loc > 50: code_score += 5

        functions = len(re.findall(r"^\s*def\s+", source_code, re.M))
        if functions > 10: code_score += 15

        udf_count = len(re.findall(r"@udf|F\.udf|pandas_udf", source_code, re.I))
        if udf_count > 0:
            code_score += min(20, udf_count * 7)
            findings.append(f"Contains {udf_count} UDF(s)")

        has_udfs = udf_count > 0
        has_streaming = bool(re.search(r"readStream|writeStream", source_code))
        if has_streaming: code_score += 20; findings.append("Streaming pipeline")

        # Data complexity
        data_score = 0.0
        join_count = len(re.findall(r"\.join\(|JOIN\s+", source_code, re.I))
        if join_count > 5: data_score += 25; findings.append(f"Complex joins: {join_count}")
        elif join_count > 2: data_score += 10

        source_count = len(re.findall(r"spark\.read|spark\.table|readStream", source_code))
        if source_count > 5: data_score += 25
        elif source_count > 2: data_score += 10

        if re.search(r"StructType|ArrayType|explode", source_code): data_score += 15

        # Dependency complexity
        dep_score = 0.0
        has_jars = bool(re.search(r"\.jar|--jars", source_code))
        if has_jars: dep_score += 30; findings.append("Custom JARs detected")

        has_hive = bool(re.search(r"enableHiveSupport|HiveContext", source_code))
        if has_hive: dep_score += 15; findings.append("Hive Metastore dependency")

        has_boto = bool(re.search(r"boto3", source_code))
        if has_boto: dep_score += 10

        # Operational complexity
        ops_score = 0.0
        sla = metadata.get("sla_hours", 0)
        if sla and sla < 1: ops_score += 30; findings.append(f"Tight SLA: {sla}h")
        elif sla and sla < 4: ops_score += 15

        # Overall score
        overall = code_score * 0.30 + data_score * 0.25 + dep_score * 0.25 + ops_score * 0.20

        if overall < 25: category = "simple"
        elif overall < 50: category = "medium"
        elif overall < 75: category = "complex"
        else: category = "critical"

        approach = "lift_and_shift" if category == "simple" else "refactor" if dep_score < 70 else "rewrite"

        score = ComplexityScore(
            overall_score=round(overall, 1),
            category=category,
            code_complexity=round(code_score, 1),
            data_complexity=round(data_score, 1),
            dependency_complexity=round(dep_score, 1),
            operational_complexity=round(ops_score, 1),
            has_custom_jars=has_jars,
            has_udfs=has_udfs,
            has_streaming=has_streaming,
            has_ml_models=bool(re.search(r"mllib|from.*ml\.", source_code)),
            has_external_apis=bool(re.search(r"requests\.", source_code)),
            uses_hive_metastore=has_hive,
            estimated_hours=self.EFFORT_MAP.get(category, 16),
            recommended_approach=approach,
            findings=findings
        )
        self.results.append(score)
        return score
