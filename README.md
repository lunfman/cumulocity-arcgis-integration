# Cumu -> ArcGIS integration

This repository contains all files for integration.

# Repository structure

- ansible: Location of ansible playbooks
- cumo: Location of Dagster logic for the integration
- grafana: configuration files and dashboards for grafana
- jobs: custom dagster workflow yaml configurations and templates
- prometheus: configuration for prometheus

# Description

The main files is `main.yaml` where full data pipeline is configured. It utilizes templates and other pipelines from jobs directory.
Modules can be created by using dagster-factory-pipelines python package, which was developed as a part of this solution.
More information can be found here: https://pypi.org/project/dagster-factory-pipelines/

# How to use?

1. Provide all required values in `.env-example`
2. Rename `.env-example` to `.env`
3. docker compose up --build
