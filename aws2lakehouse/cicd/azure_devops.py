"""Azure DevOps Pipeline Templates for Databricks Asset Bundles."""


class AzureDevOpsPipeline:
    """Generates azure-pipelines.yml for DAB deployment."""

    @staticmethod
    def generate(project_name: str, environments: list = None) -> str:
        environments = environments or ["dev", "staging", "production"]
        return """# Azure DevOps Pipeline for Databricks Asset Bundles
trigger:
  branches:
    include:
      - main
      - develop
      - feature/*

pool:
  vmImage: 'ubuntu-latest'

variables:
  - group: databricks-credentials

stages:
  - stage: Validate
    displayName: 'Validate Bundle'
    jobs:
      - job: validate
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.11'
          - script: |
              pip install databricks-cli
              databricks bundle validate
            displayName: 'Validate DAB'

  - stage: DeployStaging
    displayName: 'Deploy to Staging'
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/develop'))
    jobs:
      - deployment: staging
        environment: staging
        strategy:
          runOnce:
            deploy:
              steps:
                - script: |
                    databricks bundle deploy -t staging
                  env:
                    DATABRICKS_HOST: $(STAGING_HOST)
                    DATABRICKS_TOKEN: $(STAGING_TOKEN)

  - stage: DeployProduction
    displayName: 'Deploy to Production'
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: production
        environment: production
        strategy:
          runOnce:
            deploy:
              steps:
                - script: |
                    databricks bundle deploy -t production
                  env:
                    DATABRICKS_HOST: $(PROD_HOST)
                    DATABRICKS_TOKEN: $(PROD_TOKEN)
"""
