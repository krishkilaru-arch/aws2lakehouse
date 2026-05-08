"""Tests for aws2lakehouse.genai, dashboard, bootstrap, cicd modules."""


from aws2lakehouse.bootstrap import Bootstrap
from aws2lakehouse.cicd import BranchStrategy, DABGenerator
from aws2lakehouse.cicd.azure_devops import AzureDevOpsPipeline
from aws2lakehouse.dashboard import WavePlanningDashboard
from aws2lakehouse.genai import DebugAssistant, LineageExplainer, PipelineGenerator

# ── PipelineGenerator ────────────────────────────────────────────────────────


class TestPipelineGenerator:
    def test_from_description_heuristic(self):
        """Without a real AI endpoint, should fall back to heuristic."""
        gen = PipelineGenerator()
        yaml_out = gen.from_description(
            description="Ingest daily orders from S3 into bronze",
            domain="finance",
            catalog="production",
        )
        assert isinstance(yaml_out, str)
        assert len(yaml_out) > 0
        # Should contain pipeline-like content
        assert "name" in yaml_out.lower() or "pipeline" in yaml_out.lower() or "source" in yaml_out.lower()


# ── LineageExplainer ─────────────────────────────────────────────────────────


class TestLineageExplainer:
    def test_explain(self):
        explainer = LineageExplainer()
        lineage = {
            "source": "raw.orders",
            "target": "production.bronze.orders",
            "transforms": ["filter", "deduplicate"],
        }
        result = explainer.explain("production.bronze.orders", lineage)
        assert isinstance(result, str)
        assert len(result) > 0


# ── DebugAssistant ───────────────────────────────────────────────────────────


class TestDebugAssistant:
    def test_diagnose_known_error(self):
        assistant = DebugAssistant()
        result = assistant.diagnose(
            error_message="AnalysisException: Table or view not found: mydb.mytable",
            code="spark.table('mydb.mytable')",
        )
        assert isinstance(result, dict)
        assert "suggestion" in result or "fix" in str(result).lower() or len(result) > 0

    def test_diagnose_unknown_error(self):
        assistant = DebugAssistant()
        result = assistant.diagnose(
            error_message="SomeRandomException: xyz",
            code="print('hello')",
        )
        assert isinstance(result, dict)

    def test_common_fixes_catalog(self):
        """Verify known error patterns are configured."""
        assert hasattr(DebugAssistant, "COMMON_FIXES")
        assert len(DebugAssistant.COMMON_FIXES) > 0


# ── WavePlanningDashboard ────────────────────────────────────────────────────


class TestWavePlanningDashboard:
    def test_generate_all_sql(self):
        dashboard = WavePlanningDashboard(catalog="production", schema="migration")
        sql = dashboard.generate_all_sql()
        assert isinstance(sql, str)
        assert "CREATE" in sql
        assert "production" in sql
        # Should include inventory, wave, progress tables/views
        assert "inventory" in sql.lower() or "pipeline" in sql.lower()
        assert "wave" in sql.lower()


# ── Bootstrap ────────────────────────────────────────────────────────────────


class TestBootstrap:
    def test_generate_all(self):
        bootstrap = Bootstrap(
            org="acme",
            domains=["risk", "lending"],
            environments=["dev", "staging", "production"],
            admin_group="admins",
        )
        sql = bootstrap.generate_all()
        assert isinstance(sql, str)
        assert "CREATE" in sql
        assert "acme" in sql
        assert "risk" in sql
        assert "lending" in sql

    def test_generate_notebook(self):
        bootstrap = Bootstrap(
            org="acme",
            domains=["risk"],
            environments=["dev"],
            admin_group="admins",
        )
        notebook = bootstrap.generate_notebook()
        assert isinstance(notebook, str)
        assert "Databricks notebook" in notebook or "MAGIC" in notebook or "CREATE" in notebook

    def test_catalogs_created(self):
        bootstrap = Bootstrap(
            org="acme",
            domains=["risk"],
            environments=["dev", "production"],
            admin_group="admins",
        )
        sql = bootstrap.generate_all()
        assert "acme_dev" in sql
        # Production catalog uses org name directly
        assert "acme" in sql

    def test_governance_schema(self):
        bootstrap = Bootstrap(
            org="testorg",
            domains=["analytics"],
            environments=["dev"],
            admin_group="admins",
        )
        sql = bootstrap.generate_all()
        assert "audit" in sql.lower() or "governance" in sql.lower()


# ── DABGenerator ─────────────────────────────────────────────────────────────


class TestDABGenerator:
    def test_generate_bundle_yaml(self):
        gen = DABGenerator(
            project_name="acme-data",
            environments=["dev", "staging", "production"],
        )
        yaml_out = gen.generate_bundle_yaml()
        assert isinstance(yaml_out, str)
        assert "bundle:" in yaml_out
        assert "acme-data" in yaml_out

    def test_generate_github_actions(self):
        gen = DABGenerator(project_name="acme-data")
        yaml_out = gen.generate_github_actions()
        assert isinstance(yaml_out, str)
        assert "runs-on" in yaml_out or "jobs:" in yaml_out

    def test_generate_project_structure(self):
        gen = DABGenerator(project_name="acme-data")
        structure = gen.generate_project_structure()
        assert isinstance(structure, dict)
        assert len(structure) > 0


# ── BranchStrategy ───────────────────────────────────────────────────────────


class TestBranchStrategy:
    def test_generate_branch_protection_rules(self):
        rules = BranchStrategy.generate_branch_protection_rules()
        assert isinstance(rules, dict)
        assert len(rules) > 0

    def test_generate_codeowners(self):
        codeowners = BranchStrategy.generate_codeowners()
        assert isinstance(codeowners, str)
        assert len(codeowners) > 0


# ── AzureDevOpsPipeline ─────────────────────────────────────────────────────


class TestAzureDevOpsPipeline:
    def test_generate(self):
        yaml_out = AzureDevOpsPipeline.generate(
            project_name="acme-data",
            environments=["dev", "staging", "production"],
        )
        assert isinstance(yaml_out, str)
        assert "trigger:" in yaml_out or "stages:" in yaml_out or "pool:" in yaml_out
        assert "acme-data" in yaml_out or "databricks" in yaml_out.lower()
