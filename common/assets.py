"""
    Airflow Assets used in the ELT pipeline.
"""

from common.config import CFG
from airflow.sdk import Asset

def build_assets(cfg = CFG, option: str = "bronze") -> dict[str, Asset]:
    try:
        if option == "bronze":
            return {
                "bronze_traffic": Asset(f"s3://{cfg.s3_bucket}/{cfg.bronze_prefix}/subscriber_traffic"),
                "bronze_events": Asset(f"s3://{cfg.s3_bucket}/{cfg.bronze_prefix}/station_events"),
                "bronze_metrics": Asset(f"s3://{cfg.s3_bucket}/{cfg.bronze_prefix}/performance_metrics"),
            }
        
        elif option == "silvers_triggers":
            return {
                "silver_trigger_traffic": Asset("signal://silver/traffic"),
                "silver_trigger_events": Asset("signal://silver/events"),
                "silver_trigger_metrics": Asset("signal://silver/metrics")
            }
        
        elif option == "staging_silver_triggers":
            return {
                "staging_trigger_traffic": Asset("signal://staging/traffic"),
                "staging_trigger_events": Asset("signal://staging/events"),
                "staging_trigger_metrics": Asset("signal://staging/metrics")
            }

        elif option == "gold":
            return {
                "gold_traffic_ready": Asset("signal://gold/traffic"),
                "gold_metrics_ready": Asset("signal://gold/metrics"),
                "gold_events_ready": Asset("signal://gold/events"),
            }
        
        elif option == "gold_triggers":
            return {
                "health_hourly_ready": Asset("signal://gold/health_hourly"),
                "health_daily_ready": Asset("signal://gold/health_daily"),
                "sla_ready": Asset("signal://gold/sla_compliance")
            }
    except Exception as e:
        raise ValueError(f"Unknown option '{option}' for build_assets. Error: {str(e)}")