"""
acme_etl — Shared ETL library used by all Glue jobs at Acme Capital.
Packaged as wheel, deployed to S3, loaded by Glue via --extra-py-files.
"""
from setuptools import setup, find_packages

setup(
    name="acme_etl",
    version="3.2.1",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.3.0",
        "boto3>=1.26",
        "pyarrow>=12.0",
        "great-expectations>=0.16",
    ],
    author="Acme Capital Data Engineering",
    description="Shared ETL library: transforms, quality, SCD2, masking, auditing",
)
