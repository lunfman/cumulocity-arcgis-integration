# Cumu -> ArcGIS integration

This repository contains all files for integration.

# Repository structure

- cumo: Location of Dagster logic for the integration
- jobs: custom dagster workflow yaml configurations and templates

# Description

The main file is `main.yaml` where the complete data pipeline is configured. It uses templates and other pipelines from jobs directory. Modules can be created using the dagster-factory-pipelines python package, which was developed as part of this solution. More information can be found here: https://pypi.org/project/dagster-factory-pipelines/

# Prerequisites

Before using the provided solution, the following requirements should be met

- The user has access to ArcGIS
- The user can generate or have a valid OAuth 2.0 ArcGIS token or API key.
- The user has access to the Tartu Cumulocity instance (required for Cumulocity based data pipelines)
- User has access to the Reaalajaandmed API (required for Reaalajaandmed API data pipelines)
- Docker and Docker Compose installed on the personal computer/server

# How to use?

1. Git clone this repository
2. Provide all required values in `.env-example`
3. Rename `.env-example` to `.env`
4. docker compose up --build
