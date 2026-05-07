"""AWS2Lakehouse — Enterprise AWS to Databricks Migration Accelerator."""

from setuptools import setup, find_packages

setup(
    name="aws2lakehouse",
    version="1.0.0",
    description="Enterprise accelerator for migrating AWS EMR, Glue, and Step Functions to Databricks Lakehouse",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Lumenalta",
    author_email="krish.kilaru@lumenalta.com",
    url="https://github.com/krishkilaru-arch/aws2lakehouse",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "pyyaml>=6.0",
        "pydantic>=2.0",
    ],
    extras_require={
        "aws": ["boto3>=1.28"],
        "dev": ["pytest>=7.0", "black", "mypy", "flake8"],
        "databricks": ["databricks-sdk>=0.20"],
    },
    entry_points={
        "console_scripts": [
            "aws2lakehouse=aws2lakehouse.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Code Generators",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
