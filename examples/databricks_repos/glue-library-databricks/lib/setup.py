"""
Build: python setup.py bdist_wheel
Deploy: dbfs cp dist/acme_datalib-3.0.0-py3-none-any.whl /Volumes/production/libraries/
"""
from setuptools import setup, find_packages

setup(
    name="acme_datalib",
    version="3.0.0",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=["pyspark>=3.5.0"],  # No more boto3!
    author="Acme Capital Data Engineering",
    description="Financial data processing library (Databricks Edition)",
)
