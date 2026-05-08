"""
vendor_file_ingestion.py — Glue job: ingest vendor CSV files from S3 landing zone.
Triggered by S3 EventBridge when new files land in s3://acme-vendor-drops/
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "target_path"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read from vendor S3 landing zone
source_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [args["source_path"]],
        "recurse": True,
    },
    format_options={
        "withHeader": True,
        "separator": ",",
        "quoteChar": "\"",
    },
)

# Apply schema mapping
mapped = ApplyMapping.apply(
    frame=source_df,
    mappings=[
        ("vendor_id", "string", "vendor_id", "string"),
        ("invoice_number", "string", "invoice_number", "string"),
        ("amount", "string", "amount", "decimal(15,2)"),
        ("currency", "string", "currency", "string"),
        ("invoice_date", "string", "invoice_date", "date"),
        ("due_date", "string", "due_date", "date"),
        ("status", "string", "status", "string"),
        ("description", "string", "description", "string"),
    ],
)

# Drop nulls on required fields
filtered = DropNullFields.apply(frame=mapped)

# Write to curated zone (Parquet, partitioned by invoice_date)
glueContext.write_dynamic_frame.from_options(
    frame=filtered,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": args["target_path"],
        "partitionKeys": ["invoice_date"],
    },
)

job.commit()
