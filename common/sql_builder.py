import validators
from config import PipelineConfig as C

def sql_bronze_extractor(table_name: str, columns: list[str]) -> str:
    query = f"""
                SELECT {columns}
                FROM {C.schema_name}.{table_name}
                WHERE updated_at > %s AND updated_at <= %s
                ORDER BY updated_at
                LIMIT %s"""