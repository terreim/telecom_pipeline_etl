import logging
from typing import Optional
from datetime import datetime, timedelta

from shared.common.config import CFG

from telecom_pipeline_etl.dags.shared.util.staging_loader import ClickHouseLoader
from shared.util.silver_transformer import SilverTransformer

logger = logging.getLogger(__name__)

