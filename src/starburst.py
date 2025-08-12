from trino.dbapi import connect
from trino.auth import OAuth2Authentication
from .config import (
    TRINO_HOST, TRINO_PORT, TRINO_CATALOG, TRINO_SCHEMA, TRINO_USER, LOCAL_CERT_BUNDLE
)
import certifi

def get_conn():
    kwargs = dict(
        host=TRINO_HOST,
        port=TRINO_PORT,
        http_scheme="https",
        auth=OAuth2Authentication(),   # triggers SSO via browser
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    # TLS verification strategy:
    if LOCAL_CERT_BUNDLE:       # corporate bundle provided
        kwargs["verify"] = LOCAL_CERT_BUNDLE
    else:                       # fall back to certifi bundle
        kwargs["verify"] = certifi.where()

    return connect(**kwargs)

def run_query(sql: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return cols, rows
