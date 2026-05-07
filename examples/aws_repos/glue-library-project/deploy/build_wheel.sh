#!/bin/bash
# Build and upload the acme_etl wheel to S3 for Glue jobs
set -e

echo "Building acme_etl wheel..."
cd lib/
python -m build --wheel
WHEEL=$(ls dist/*.whl | head -1)

echo "Uploading $WHEEL to S3..."
aws s3 cp "$WHEEL" s3://acme-libs/acme_etl-3.2.1-py3-none-any.whl

echo "Updating Glue jobs to use new wheel..."
for job in customer_dimension order_events_processing financial_reconciliation feature_store_builder vendor_data_ingestion; do
    aws glue update-job --job-name "$job" --job-update \
        "Command={ScriptLocation=s3://acme-scripts/$job.py,PythonVersion=3},DefaultArguments={--extra-py-files=s3://acme-libs/acme_etl-3.2.1-py3-none-any.whl}"
done

echo "✅ Deploy complete"
