"""Tests for aws2lakehouse.governance module."""


from aws2lakehouse.governance import (
    DataQualityGovernance,
    MNPIController,
    UnityCatalogSetup,
)


class TestUnityCatalogSetup:
    def setup_method(self):
        self.uc = UnityCatalogSetup(
            org="acme",
            environments=["dev", "staging", "production"],
            domains=["risk", "lending"],
            layers=["bronze", "silver", "gold"],
        )

    def test_generate_setup_sql(self):
        sql = self.uc.generate_setup_sql()
        assert "CREATE CATALOG" in sql
        assert "acme_dev" in sql
        assert "acme_staging" in sql
        assert "risk_bronze" in sql
        assert "lending_silver" in sql

    def test_generate_volume_sql(self):
        sql = self.uc.generate_volume_sql([
            {"name": "raw-data", "environment": "dev"},
            {"name": "processed", "environment": "production"},
        ])
        assert "VOLUME" in sql or "LOCATION" in sql or "raw-data" in sql

    def test_generate_access_policies(self):
        policies = self.uc.generate_access_policies()
        assert len(policies) > 0
        # All policies should have non-empty securable_name (bug fix)
        for p in policies:
            assert p.securable_name, f"Empty securable_name for {p.principal}/{p.privilege}"

    def test_grants_include_all_environments(self):
        """Bug fix: all environments should produce valid GRANT statements."""
        sql = self.uc.generate_grants_sql()
        assert "acme_dev" in sql
        assert "acme_staging" in sql
        assert "acme_production" in sql

    def test_grants_have_engineer_access(self):
        sql = self.uc.generate_grants_sql()
        assert "acme_data_engineers" in sql
        assert "ALL PRIVILEGES" in sql

    def test_grants_have_analyst_access(self):
        sql = self.uc.generate_grants_sql()
        assert "acme_analysts" in sql
        assert "SELECT" in sql


class TestMNPIController:
    def setup_method(self):
        self.mnpi = MNPIController(catalog="production")

    def test_generate_mnpi_tags_sql(self):
        tables = [
            {"table": "production.risk.trades", "columns": ["price", "volume"]},
        ]
        sql = self.mnpi.generate_mnpi_tags_sql(tables)
        assert "data_classification" in sql
        assert "production.risk.trades" in sql

    def test_generate_row_filter_sql(self):
        sql = self.mnpi.generate_row_filter_sql("production.risk.trades", "embargo_date")
        assert "ROW FILTER" in sql or "FUNCTION" in sql

    def test_generate_audit_table_sql(self):
        sql = self.mnpi.generate_audit_table_sql()
        assert "audit" in sql.lower()
        assert "USING DELTA" in sql

    def test_generate_column_mask_sql(self):
        configs = [
            {
                "table": "production.lending.loans",
                "columns": [
                    {
                        "name": "ssn", "data_type": "STRING",
                        "mask_expression": "'***-**-****'",
                        "approved_group": "pii_approved",
                    }
                ],
            },
        ]
        sql = self.mnpi.generate_column_mask_sql(configs)
        assert "ssn" in sql or "loans" in sql

    def test_generate_dynamic_views_sql(self):
        configs = [
            {"source_table": "production.risk.raw", "view_name": "production.risk.filtered", "filter": "region = 'US'"},
        ]
        sql = self.mnpi.generate_dynamic_views_sql(configs)
        assert "VIEW" in sql


class TestDataQualityGovernance:
    def test_generate_expectations_sql(self):
        dqg = DataQualityGovernance()
        rules = [
            {"name": "positive_amount", "condition": "amount > 0", "action": "warn"},
            {"name": "not_null_id", "condition": "id IS NOT NULL", "action": "fail"},
        ]
        sql = dqg.generate_expectations_sql("orders", rules)
        assert "CONSTRAINT" in sql or "EXPECT" in sql
