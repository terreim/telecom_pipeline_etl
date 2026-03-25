from datetime import timedelta

_BASE = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

BRONZE_DEFAULTS = {**_BASE, "execution_timeout": timedelta(minutes=10), "sla": timedelta(hours=1)}
SILVER_DEFAULTS = {**_BASE, "execution_timeout": timedelta(minutes=30), "retries": 3}
GOLD_DEFAULTS   = {**_BASE, "execution_timeout": timedelta(hours=1), "retries": 5}
STAGING_DEFAULTS = {**_BASE, "execution_timeout": timedelta(minutes=30), "retries": 3}
RECOVERY_DEFAULTS = {**_BASE, "retries": 1}