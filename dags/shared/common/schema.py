import json
from typing import Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Maps Postgres type names (lowercase) to their canonical PyArrow types.
# decimal/numeric are intentionally absent — the existing decimal logic handles them.
# timestamp/timestamptz are intentionally absent — the existing timestamp logic handles them.
_PG_TYPE_TO_ARROW: dict[str, pa.DataType] = {
    "smallint":         pa.int16(),
    "integer":          pa.int64(),
    "int":              pa.int64(),
    "int2":             pa.int16(),
    "int4":             pa.int32(),
    "int8":             pa.int64(),
    "bigint":           pa.int64(),
    "serial":           pa.int32(),
    "bigserial":        pa.int64(),
    "real":             pa.float32(),
    "float4":           pa.float32(),
    "float8":           pa.float64(),
    "double precision": pa.float64(),
    "boolean":          pa.bool_(),
    "bool":             pa.bool_(),
    "text":             pa.string(),
    "varchar":          pa.string(),
    "char":             pa.string(),
    "json":             pa.string(),
    "jsonb":            pa.string(),
}


def unify_schema(
    pyarrow_schema: pa.Schema,
    pg_type_map: Optional[dict[str, str]] = None,
) -> pa.Schema:
    """Normalize a PyArrow schema for consistent Parquet writes.

    - Columns present in *pg_type_map* are cast to their canonical Arrow type,
      preventing Pandas NULL-promotion (e.g. integer → float64 when NULLs exist).
    - Decimal columns are widened to precision 38 so chunks never conflict.
    - Timestamp columns are normalised to microsecond precision for Spark.
    - All other columns pass through unchanged.
    """
    fields = []
    for field in pyarrow_schema:
        canonical = None
        if pg_type_map and field.name in pg_type_map:
            pg_type = pg_type_map[field.name].lower().strip()
            canonical = _PG_TYPE_TO_ARROW.get(pg_type)

        if canonical is not None:
            fields.append(pa.field(field.name, canonical, nullable=True))
        elif pa.types.is_decimal(field.type):
            fields.append(pa.field(field.name, pa.decimal128(38, field.type.scale), nullable=True))
        elif pa.types.is_timestamp(field.type):
            fields.append(pa.field(field.name, pa.timestamp('us'), nullable=True))
        else:
            fields.append(pa.field(field.name, field.type, nullable=True))
    return pa.schema(fields)
            
def serialize_jsonb_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Serialize any dict/JSONB columns to JSON strings."""
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x
            )
    return df