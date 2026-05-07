"""
acme_datalib — Shared data processing library for Acme Capital's Glue ETL jobs.

Build: python setup.py bdist_wheel
Deploy: aws s3 cp dist/acme_datalib-2.1.0-py3-none-any.whl s3://acme-artifacts/libs/
Usage in Glue: --extra-py-files s3://acme-artifacts/libs/acme_datalib-2.1.0-py3-none-any.whl
"""
from setuptools import setup, find_packages

setup(
    name="acme_datalib",
    version="2.1.0",
    packages=find_packages(where="lib"),
    package_dir={"": "lib"},
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.3.0",
        "boto3>=1.26.0",
        "pandas>=1.5.0",
        "numpy>=1.23.0",
        "pyarrow>=10.0.0",
        "cryptography>=38.0.0",
    ],
    extras_require={
        "dev": ["pytest", "moto", "pytest-cov", "black", "mypy"],
    },
    author="Acme Capital Data Engineering",
    description="Shared data processing library for financial ETL pipelines",
)
