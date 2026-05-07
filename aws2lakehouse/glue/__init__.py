"""
Glue Module — AWS Glue ETL migration to Databricks.

Ported from glue2lakehouse accelerator with enhancements:
- DynamicFrame → DataFrame conversion
- GlueContext → SparkSession
- Glue Catalog → Unity Catalog
- Job Bookmarks → Delta CDF / watermarking
- S3 paths → Unity Catalog Volumes
- Glue Connections → Databricks Secrets + JDBC

Usage:
    from aws2lakehouse.glue import GlueCodeTransformer
    
    transformer = GlueCodeTransformer(target_catalog="production")
    migrated_code = transformer.transform(glue_script_content)
"""

# Re-export from glue2lakehouse if available, otherwise provide minimal implementation
try:
    import sys
    sys.path.insert(0, '/Workspace/Users/krish.kilaru@lumenalta.com/glue2lakehouse')
    from glue2lakehouse.core.transformer import CodeTransformer as GlueCodeTransformer
    from glue2lakehouse.migration.workflow_migrator import WorkflowMigrator as GlueWorkflowMigrator
except ImportError:
    # Minimal standalone implementation
    import re
    
    class GlueCodeTransformer:
        """Transforms AWS Glue PySpark code to Databricks."""
        
        def __init__(self, target_catalog: str = "production"):
            self.target_catalog = target_catalog
            self._warnings = []
        
        def transform(self, source_code: str) -> str:
            """Transform Glue code to Databricks."""
            code = source_code
            
            # Remove Glue imports
            code = re.sub(r'from awsglue\..*\n', '', code)
            code = re.sub(r'import awsglue.*\n', '', code)
            
            # Replace GlueContext
            code = re.sub(r'GlueContext\(.*\)', 'SparkSession.builder.getOrCreate()', code)
            code = re.sub(r'glueContext\.spark_session', 'spark', code)
            
            # Replace DynamicFrame operations
            code = re.sub(r'DynamicFrame\.fromDF\(([^,]+),.*\)', r'\1', code)
            code = re.sub(r'(\.toDF\(\))', '', code)
            code = re.sub(r'DynamicFrame', 'DataFrame', code)
            
            # Replace catalog operations
            code = re.sub(
                r'create_dynamic_frame\.from_catalog\(\s*database\s*=\s*([^,]+),\s*table_name\s*=\s*([^,)]+)',
                f'spark.table(f"{self.target_catalog}.{{\\1}}.{{\\2}}"',
                code
            )
            
            # Remove Job class
            code = re.sub(r'job\.init\(.*\)', '# Job initialization handled by Databricks', code)
            code = re.sub(r'job\.commit\(\)', '# Job commit handled by Databricks', code)
            
            # S3 → Volumes
            code = re.sub(
                r's3://([\w-]+)/',
                f'/Volumes/{self.target_catalog}/external/\\1/',
                code
            )
            
            # Add header
            header = f"""# ==============================================================================
# MIGRATED FROM AWS GLUE TO DATABRICKS
# Target Catalog: {self.target_catalog}
# ==============================================================================

"""
            return header + code
        
        def get_warnings(self) -> list:
            return self._warnings
    
    class GlueWorkflowMigrator:
        """Placeholder for Glue workflow migration."""
        pass
