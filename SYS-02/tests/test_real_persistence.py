import os
import sys
from unittest.mock import Mock, patch

import pytest
import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator

RECORD = {"symbol": "BTCUSDT", "timestamp": 1724472000, "open": 64000, "high": 64500, "low": 63900, "close": 64200, "volume": 12.5}


def test_minio_failure_fails_task_instead_of_returning_mock_success():
    operator = LakehouseTaskOperator(endpoint="unreachable:9000")
    with patch.object(operator, "_client", side_effect=OSError("network unavailable")):
        with pytest.raises(RuntimeError, match="MinIO write failed"):
            operator.write_bronze_klines([RECORD], "BTCUSDT", "2026-08-24")


def test_minio_success_writes_bronze_object():
    client = Mock()
    client.bucket_exists.return_value = True
    operator = LakehouseTaskOperator()
    with patch.object(operator, "_client", return_value=client):
        result = operator.write_bronze_klines([RECORD], "BTCUSDT", "2026-08-24")
    assert result["bronze_output_path"].endswith("bronze.json")
    client.put_object.assert_called_once()


def test_clickhouse_failure_fails_task_instead_of_returning_mock_success():
    operator = ClickHouseTaskOperator(host="unreachable")
    with patch("sys02_orchestration.operators.clickhouse_operator.requests.post", side_effect=Exception("network unavailable")):
        with pytest.raises(RuntimeError, match="ClickHouse query failed"):
            operator.execute_sql("SELECT 1")


def test_bronze_cleanup_deletes_only_expired_objects():
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace
    client = Mock()
    client.bucket_exists.return_value = True
    now = datetime.now(timezone.utc)
    client.list_objects.return_value = [
        SimpleNamespace(object_name="expired.json", last_modified=now - timedelta(days=31), size=12),
        SimpleNamespace(object_name="recent.json", last_modified=now - timedelta(days=29), size=34),
    ]
    operator = LakehouseTaskOperator()
    with patch.object(operator, "_client", return_value=client):
        result = operator.clean_expired_bronze_files(retention_days=30)
    assert result == {"bucket": "crypto-bronze", "retention_days": 30, "files_purged": 1, "reclaimed_bytes": 12, "status": "SUCCESS"}
    client.remove_object.assert_called_once_with("crypto-bronze", "expired.json")
