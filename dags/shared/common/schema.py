import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def unify_schema(pyarrow_schema: pa.Schema) -> pa.Schema:
    """
        Normalize decimal precisions to max(38,s) so chunks never conflict. 
        Also convert timestamp to microsecond precision for Spark compatibility.
    """
    fields = []
    for field in pyarrow_schema:
        if pa.types.is_decimal(field.type):
            # Widen to max precision, keep original scale
            fields.append(pa.field(field.name, pa.decimal128(38, field.type.scale), nullable=True))

        elif pa.types.is_timestamp(field.type):
            # Convert to microsecond precision (Spark's default)
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