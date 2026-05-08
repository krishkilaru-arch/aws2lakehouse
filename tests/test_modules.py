"""Tests for auxiliary modules: complexity analyzer, wave planner, etc."""

import pytest

from aws2lakehouse.discovery.complexity_analyzer import ComplexityAnalyzer, ComplexityScore


class TestComplexityAnalyzer:
    @pytest.fixture
    def analyzer(self):
        return ComplexityAnalyzer()

    def test_simple_code(self, analyzer):
        code = """
df = spark.read.format("csv").load("s3://bucket/data")
df.write.format("delta").saveAsTable("catalog.schema.table")
"""
        score = analyzer.analyze_code(code)
        assert isinstance(score, ComplexityScore)
        assert score.category in ("simple", "medium")

    def test_complex_code(self, analyzer):
        code = """
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType

@udf
def custom_transform(val):
    return val.upper()

@udf
def another_udf(val):
    return val.lower()

@udf
def third_udf(val):
    return val.strip()

df1 = spark.readStream.format("kafka").load()
df2 = spark.table("ref_table")
df3 = spark.read.format("jdbc").load()
df4 = spark.table("lookup")
df5 = spark.table("dim_table")
df6 = spark.table("fact_table")

result = (df1.join(df2, "key")
          .join(df3, "id")
          .join(df4, "code")
          .join(df5, "dim_key")
          .join(df6, "fact_key")
          .join(df6, "fact_key2")
          .withColumn("exploded", F.explode("nested")))
result.writeStream.format("delta").start()
""" + "# padding\\n" * 300  # push LOC up
        score = analyzer.analyze_code(code)
        assert score.has_streaming
        assert score.has_udfs
        assert score.overall_score > 20  # has meaningful complexity
        assert len(score.findings) > 0

    def test_jar_dependency_detected(self, analyzer):
        code = "spark-submit --jars s3://bucket/custom.jar main.py"
        score = analyzer.analyze_code(code)
        assert score.has_custom_jars

    def test_hive_detected(self, analyzer):
        code = "spark = SparkSession.builder.enableHiveSupport().getOrCreate()"
        score = analyzer.analyze_code(code)
        assert score.uses_hive_metastore

    def test_effort_hours_positive(self, analyzer):
        code = "x = 1\n" * 100
        score = analyzer.analyze_code(code)
        assert score.estimated_hours >= 0


class TestROICalculator:
    def test_calculate_returns_dict(self):
        from aws2lakehouse.roi_calculator import AWSCurrentState, DatabricksProjectedState, ROICalculator
        aws = AWSCurrentState(emr_monthly_cost=10000, glue_monthly_cost=5000)
        dbx = DatabricksProjectedState(jobs_dbu_monthly=8000)
        calc = ROICalculator(current=aws, projected=dbx, migration_cost=50000)
        result = calc.calculate()
        assert isinstance(result, dict)
        assert "summary" in result
        assert "breakdown" in result

    def test_executive_summary_not_empty(self):
        from aws2lakehouse.roi_calculator import AWSCurrentState, DatabricksProjectedState, ROICalculator
        aws = AWSCurrentState(emr_monthly_cost=10000)
        dbx = DatabricksProjectedState(jobs_dbu_monthly=5000)
        calc = ROICalculator(current=aws, projected=dbx, migration_cost=30000)
        summary = calc.generate_executive_summary()
        assert len(summary) > 50


class TestConfig:
    def test_default_config(self):
        from aws2lakehouse.utils.config import Config
        cfg = Config()
        assert cfg.get("target_catalog") == "production"

    def test_custom_config(self, tmp_dir):
        import os
        from pathlib import Path

        from aws2lakehouse.utils.config import Config

        config_path = os.path.join(tmp_dir, "config.yaml")
        Path(config_path).write_text("target_catalog: my_catalog\n")
        cfg = Config(config_path)
        assert cfg.get("target_catalog") == "my_catalog"


class TestLogger:
    def test_setup_logger(self):
        from aws2lakehouse.utils.logger import setup_logger
        log = setup_logger("test_logger")
        assert log.name == "test_logger"
        assert len(log.handlers) >= 1
