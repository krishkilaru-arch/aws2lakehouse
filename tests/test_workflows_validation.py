"""Tests for aws2lakehouse.workflows and aws2lakehouse.validation modules."""


from aws2lakehouse.validation import (
    MigrationValidator,
    ValidationReport,
    ValidationResult,
)
from aws2lakehouse.workflows import (
    FolderStructureGenerator,
    WorkflowTask,
    WorkflowTemplates,
)

# ── WorkflowTemplates ────────────────────────────────────────────────────────


class TestWorkflowTemplates:
    def test_medallion_pipeline(self):
        yaml_out = WorkflowTemplates.medallion_pipeline(
            name="orders_pipeline",
            domain="finance",
            tables=["orders", "payments"],
            catalog="production",
            schedule="0 6 * * *",
        )
        assert isinstance(yaml_out, str)
        assert "orders_pipeline" in yaml_out
        assert "finance" in yaml_out

    def test_conditional_workflow(self):
        yaml_out = WorkflowTemplates.conditional_workflow(
            name="conditional_etl",
            domain="risk",
            check_notebook="/Workspace/notebooks/check",
            success_notebook="/Workspace/notebooks/success",
            failure_notebook="/Workspace/notebooks/failure",
        )
        assert isinstance(yaml_out, str)
        assert "conditional_etl" in yaml_out

    def test_for_each_workflow(self):
        yaml_out = WorkflowTemplates.for_each_workflow(
            name="multi_table",
            domain="lending",
            tables=["loans", "applications", "payments"],
            notebook_path="/Workspace/notebooks/ingest",
            schedule="0 2 * * *",
        )
        assert isinstance(yaml_out, str)
        assert "multi_table" in yaml_out

    def test_backfill_workflow(self):
        yaml_out = WorkflowTemplates.backfill_workflow(
            name="backfill_orders",
            table="production.bronze.orders",
            start_date="2024-01-01",
            end_date="2024-12-31",
            notebook_path="/Workspace/notebooks/backfill",
        )
        assert isinstance(yaml_out, str)
        assert "backfill" in yaml_out.lower()

    def test_dual_write_cutover(self):
        yaml_out = WorkflowTemplates.dual_write_cutover(
            name="cutover_orders",
            domain="finance",
            legacy_notebook="/Workspace/notebooks/legacy",
            new_notebook="/Workspace/notebooks/new",
            validation_notebook="/Workspace/notebooks/validate",
        )
        assert isinstance(yaml_out, str)
        assert "cutover" in yaml_out.lower() or "dual" in yaml_out.lower()


# ── FolderStructureGenerator ─────────────────────────────────────────────────


class TestFolderStructureGenerator:
    def test_generate(self):
        result = FolderStructureGenerator.generate(
            org="acme",
            domains=["risk", "lending"],
            environments=["dev", "staging", "production"],
        )
        assert isinstance(result, (dict, str))
        assert len(result) > 0

    def test_generate_setup_script(self):
        script = FolderStructureGenerator.generate_setup_script(
            org="acme",
            domains=["risk", "lending"],
        )
        assert isinstance(script, str)
        assert "mkdir" in script.lower() or "os.makedirs" in script.lower() or len(script) > 0


# ── WorkflowTask Dataclass ───────────────────────────────────────────────────


class TestWorkflowTask:
    def test_defaults(self):
        task = WorkflowTask(task_key="t1", notebook_path="/path")
        assert task.task_key == "t1"
        assert task.depends_on == [] or not task.depends_on


# ── MigrationValidator ───────────────────────────────────────────────────────


class TestMigrationValidator:
    def test_generate_validation_notebook(self):
        validator = MigrationValidator()
        notebook = validator.generate_validation_notebook(
            pipeline_name="orders_etl",
            source_table="aws_db.orders",
            target_table="production.bronze.orders",
            primary_key="order_id",
        )
        assert isinstance(notebook, str)
        assert "orders" in notebook
        assert "order_id" in notebook

    def test_generate_validation_with_numeric_columns(self):
        validator = MigrationValidator()
        notebook = validator.generate_validation_notebook(
            pipeline_name="test",
            source_table="src.t",
            target_table="tgt.t",
            primary_key="id",
            numeric_columns=["amount", "quantity"],
        )
        assert "amount" in notebook
        assert "quantity" in notebook

    def test_generate_batch_validation_notebook(self):
        validator = MigrationValidator()
        validations = [
            {
                "pipeline_name": "orders",
                "source_table": "src.orders",
                "target_table": "tgt.orders",
                "primary_key": "id",
            },
            {
                "pipeline_name": "customers",
                "source_table": "src.customers",
                "target_table": "tgt.customers",
                "primary_key": "cust_id",
            },
        ]
        notebook = validator.generate_batch_validation_notebook(validations)
        assert isinstance(notebook, str)
        assert "orders" in notebook
        assert "customers" in notebook

    def test_generate_business_rule_tests(self):
        validator = MigrationValidator()
        rules = [
            {"name": "positive_amount", "condition": "amount > 0"},
            {"name": "valid_status", "condition": "status IN ('active', 'closed')"},
        ]
        code = validator.generate_business_rule_tests("production.bronze.orders", rules)
        assert isinstance(code, str)
        assert "positive_amount" in code

    def test_generate_cutover_checklist(self):
        validator = MigrationValidator()
        checklist = validator.generate_cutover_checklist("orders_etl", "finance")
        assert isinstance(checklist, str)
        assert "orders_etl" in checklist
        assert "finance" in checklist


# ── ValidationResult / ValidationReport ──────────────────────────────────────


class TestValidationReport:
    def test_empty_report(self):
        report = ValidationReport(pipeline_name="test", source_table="s", target_table="t", results=[])
        assert report.passed == 0
        assert report.failed == 0

    def test_report_with_results(self):
        checks = [
            ValidationResult(
                check_name="row_count", status="PASS",
                source_value="100", target_value="100",
                difference="0", details="OK",
            ),
            ValidationResult(
                check_name="schema", status="PASS",
                source_value="10", target_value="10",
                difference="0", details="OK",
            ),
            ValidationResult(
                check_name="nulls", status="FAIL",
                source_value="0", target_value="3",
                difference="3", details="3 nulls",
            ),
        ]
        report = ValidationReport(pipeline_name="test", source_table="s", target_table="t", results=checks)
        assert report.passed == 2
        assert report.failed == 1

    def test_summary(self):
        checks = [
            ValidationResult(
                check_name="row_count", status="PASS",
                source_value="100", target_value="100",
                difference="0", details="OK",
            ),
        ]
        report = ValidationReport(pipeline_name="test", source_table="s", target_table="t", results=checks)
        summary = report.to_summary()
        assert isinstance(summary, str)
        assert "test" in summary or "row_count" in summary
