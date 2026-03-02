import logging
from typing import Optional
from datetime import datetime, timedelta
import pytz

from airflow.sdk import DAG, task
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
import os

from config.config import PipelineConfig as C

from util.warehouse_loader import ClickHouseLoader
from util.silver_transformer import SilverTransformer

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-team", 
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
    "sla": timedelta(hours=2),
}
