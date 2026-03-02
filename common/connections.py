from contextlib import contextmanager

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def get_s3_hook(conn_id: str) -> S3Hook:
    return S3Hook(aws_conn_id=conn_id)

def get_s3_credentials(conn_id: str) -> tuple[str, str, str]:
    s3_hook = get_s3_hook(conn_id=conn_id)
    
    creds = s3_hook.get_credentials()
    s3_access_key = creds.access_key
    s3_secret_key = creds.secret_key

    conn = s3_hook.get_connection(conn_id=conn_id)
    s3_endpoint = conn.extra_dejson.get("endpoint_url", "http://minio:9000")

    return (s3_access_key, s3_secret_key, s3_endpoint)

@contextmanager
def pg_cursor(pg_hook: PostgresHook):
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        yield conn, cursor
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
