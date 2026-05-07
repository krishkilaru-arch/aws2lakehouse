#!/bin/bash
# Build the acme_datalib wheel and deploy to S3
# Usage: ./scripts/build_and_deploy.sh [staging|prod]

set -euo pipefail

ENVIRONMENT=${1:-staging}
ARTIFACT_BUCKET="acme-artifacts"
LIB_VERSION=$(python -c "import importlib.metadata; print(importlib.metadata.version('acme_datalib'))" 2>/dev/null || echo "2.1.0")

echo "Building acme_datalib v${LIB_VERSION} for ${ENVIRONMENT}..."

# Clean previous builds
rm -rf dist/ build/ *.egg-info

# Build wheel
python setup.py bdist_wheel --universal

# Upload to S3
WHEEL_FILE=$(ls dist/*.whl)
S3_PATH="s3://${ARTIFACT_BUCKET}/libs/${ENVIRONMENT}/$(basename ${WHEEL_FILE})"

echo "Uploading ${WHEEL_FILE} → ${S3_PATH}"
aws s3 cp "${WHEEL_FILE}" "${S3_PATH}"

# Also upload as "latest" for Glue jobs that don't pin versions
aws s3 cp "${WHEEL_FILE}" "s3://${ARTIFACT_BUCKET}/libs/${ENVIRONMENT}/acme_datalib-latest.whl"

echo "✅ Deploy complete: ${S3_PATH}"
echo ""
echo "Glue Job Parameter:"
echo "  --extra-py-files ${S3_PATH}"
