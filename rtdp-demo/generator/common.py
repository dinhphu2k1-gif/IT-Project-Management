"""Kết nối dùng chung cho generator.py và reconcile.py. Cấu hình qua biến môi trường."""

import base64
import json
import os
import urllib.parse
import urllib.request

import oracledb

ORACLE_DSN = os.getenv("ORACLE_DSN", "localhost:1521/FREEPDB1")
ORACLE_USER = os.getenv("ORACLE_USER", "core")
ORACLE_PASSWORD = os.getenv("CORE_PASSWORD", "core")

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "rtdp")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "rtdp")


def oracle_connect():
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)


# Gọi ClickHouse trực tiếp, bỏ qua proxy hệ thống (nếu có)
_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def ch_query(sql):
    """Chạy câu SELECT trên ClickHouse qua HTTP, trả về list dict."""
    url = CLICKHOUSE_URL + "/?" + urllib.parse.urlencode({"default_format": "JSON"})
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    token = base64.b64encode(f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}".encode()).decode()
    req.add_header("Authorization", "Basic " + token)
    with _opener.open(req, timeout=10) as resp:
        return json.loads(resp.read())["data"]
