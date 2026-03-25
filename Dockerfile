# Dockerfile summary:
# - Base image: apache/airflow:3.1.5 — provides the Airflow runtime and environment.
# - USER root: temporarily escalates privileges to install OS-level packages.
# - Installs build-essential via apt-get:
#     * Provides compilers and headers needed to build native Python extensions or wheels.
#     * Uses --no-install-recommends to avoid extra packages.
# - Cleans apt caches and removes /var/lib/apt/lists/* to reduce final image size.
# - USER airflow: drops to the non-root Airflow user for subsequent operations and runtime.
# - Installs Python packages with pip (--no-cache-dir):
#     * apache-airflow-providers-amazon==8.28.0
#     * apache-airflow-providers-clickhouse==1.5.2
#   Pinning versions produces reproducible builds; --no-cache-dir prevents storing pip cache in the image.
# Notes and recommendations:
# - Keep provider versions compatible with the Airflow core version; test upgrades in a staging environment.
# - Minimize layers where practical (combine RUN commands) to optimize image size and layer count.
# - If providers require additional system libraries (e.g., libssl-dev, libsasl2-dev), add them to the apt install step.
# - Maintain principle of least privilege: install system packages as root, then revert to the airflow user for runtime.
# - Avoid embedding secrets or credentials into the image; provide them at deploy/runtime via environment variables or secret stores.

FROM apache/airflow:3.1.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon==9.19.0 \
    airflow-clickhouse-plugin==1.6.0 \
    clickhouse-driver==0.2.10 \
    pyspark==4.0.2