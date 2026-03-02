# airflow/Dockerfile
#
# Custom Airflow image with EchoStream's code and dependencies.
# Extends the official image (which has Airflow but not our packages).

FROM apache/airflow:2.10.5-python3.11

USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock /opt/airflow/

WORKDIR /opt/airflow

# Install into /home/airflow/.local — the only path Airflow's Python searches
RUN uv pip install --no-cache --target /home/airflow/.local/lib/python3.11/site-packages -r pyproject.toml

COPY src/ /opt/airflow/src/

ENV PYTHONPATH="/opt/airflow/src"
