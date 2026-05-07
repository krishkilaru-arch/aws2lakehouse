"""
payment_processing.py — Glue job: extract and transform payment data every 15 minutes.
Incremental extract from PostgreSQL using watermark column (processed_at).
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "connection_name", "bookmark_key"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read from PostgreSQL with job bookmarks (incremental)
payments_df = glueContext.create_dynamic_frame.from_catalog(
    database="lending_db",
    table_name="payments",
    transformation_ctx="payments_source",
    additional_options={
        "jobBookmarkKeys": ["payment_id"],
        "jobBookmarkKeysSortOrder": "asc",
    },
)

# Convert to DataFrame for transformations
df = payments_df.toDF()

# Business logic
df_transformed = (
    df
    .withColumn("payment_amount_usd", 
        F.when(F.col("currency") == "USD", F.col("amount"))
         .otherwise(F.col("amount") * F.col("fx_rate")))
    .withColumn("is_late", 
        F.when(F.col("payment_date") > F.col("due_date"), True)
         .otherwise(False))
    .withColumn("days_overdue",
        F.when(F.col("is_late"), F.datediff(F.col("payment_date"), F.col("due_date")))
         .otherwise(0))
    .withColumn("_processed_at", F.current_timestamp())
    # Mask account number (PII)
    .withColumn("account_number_masked", 
        F.concat(F.lit("****"), F.substring(F.col("account_number"), -4, 4)))
)

# Write to S3
output_dyf = DynamicFrame.fromDF(df_transformed, glueContext, "output")
glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://acme-data-lake/curated/payments/",
        "partitionKeys": ["payment_date"],
    },
)

job.commit()
