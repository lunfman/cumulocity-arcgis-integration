# Cumu -> ArcGIS integration

This repository contains all files for integration.

# Repository structure

- cumo: Location of Dagster logic for the integration
- jobs: custom dagster workflow yaml configurations and templates

# Description

The main file is `main.yaml` where the complete data pipeline is configured. It uses templates and other pipelines from jobs directory. Modules can be created using the dagster-factory-pipelines python package, which was developed as part of this solution. More information can be found here: https://pypi.org/project/dagster-factory-pipelines/

# How to use?

1. Provide all required values in `.env-example`
2. Rename `.env-example` to `.env`
3. docker compose up --build
