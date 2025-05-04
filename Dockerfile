# Some parts are copied from dagster guide
# https://docs.dagster.io/guides/deploy/deployment-options/docker
# Dagster server and daemon image
FROM python:3.10-slim

RUN pip install \
    dagster==1.10.13  \
    dagster-docker==0.26.13  \
    dagster-graphql==1.10.13  \
    dagster-postgres==0.26.13  \
    dagster-webserver==1.10.13  \
    dagster-aws==0.26.13 \
    aiohttp==3.11.18 

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

RUN mkdir -p $DAGSTER_HOME

COPY dagster-container.yaml $DAGSTER_HOME/dagster.yaml
COPY workspace-container.yaml $DAGSTER_HOME/workspace.yaml

WORKDIR $DAGSTER_HOME
