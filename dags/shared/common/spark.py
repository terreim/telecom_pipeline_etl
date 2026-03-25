"""
Spark session factory with MinIO/S3A configuration.

Usage:
    from shared.common.spark import get_spark_session
    spark = get_spark_session("silver_transformer")
"""

from pyspark.sql import SparkSession

from shared.common.connections import get_s3_credentials
from shared.common.config import PipelineConfig


def get_spark_session(
    app_name: str,
    master: str = "spark://spark-master:7077",
    s3_conn_id: str | None = None,
) -> SparkSession:
    cfg = PipelineConfig.from_env()
    s3_conn_id = s3_conn_id or cfg.s3_conn_id

    access_key, secret_key, endpoint = get_s3_credentials(s3_conn_id)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # S3A filesystem for MinIO
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Parquet defaults
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Force Airflow to read jars
        .config("spark.driver.extraClassPath", "/opt/spark-extra-jars/*")
        .config("spark.executor.extraClassPath", "/opt/spark-extra-jars/*")
        # Force worker and driver to use Python3.12
        .config("spark.pyspark.python", "/usr/bin/python3.12")
        .config("spark.pyspark.driver.python", "/usr/bin/python3.12")
        .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3.12")
        .config("spark.pyspark.worker.reuse", "false")

    )

    return builder.getOrCreate()
