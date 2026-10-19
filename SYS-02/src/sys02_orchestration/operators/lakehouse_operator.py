"""Real MinIO-backed Bronze and Silver lakehouse operations."""

import io
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from minio import Minio
from sys02_orchestration.config import config


class LakehouseTaskOperator:
    def __init__(self, endpoint: str = config.lakehouse.minio_endpoint):
        self.endpoint = endpoint
        self.access_key = config.lakehouse.minio_access_key
        self.secret_key = config.lakehouse.minio_secret_key
        self.bronze_bucket = config.lakehouse.bronze_bucket
        self.silver_bucket = config.lakehouse.silver_bucket

    def _client(self) -> Minio:
        # host.docker.internal is intentionally preserved: this code runs inside
        # an Airflow container and must reach the SYS-05 service on the host.
        return Minio(self.endpoint, access_key=self.access_key, secret_key=self.secret_key, secure=False)

    def _write(self, bucket: str, object_name: str, payload: Dict[str, Any]) -> str:
        encoded = json.dumps(payload, default=str).encode("utf-8")
        try:
            client = self._client()
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
            client.put_object(bucket, object_name, io.BytesIO(encoded), len(encoded), content_type="application/json")
        except Exception as exc:
            raise RuntimeError(f"MinIO write failed for s3://{bucket}/{object_name}: {exc}") from exc
        return f"s3://{bucket}/{object_name}"

    def write_bronze_klines(self, records: List[Dict[str, Any]], symbol: str, date_str: str, source: str = "v1.0.0_api_klines") -> Dict[str, Any]:
        if not records:
            raise ValueError("Cannot write an empty Bronze batch")
        path = self._write(self.bronze_bucket, f"klines/symbol={symbol}/date={date_str}/bronze.json", {"symbol": symbol, "source": source, "records": records})
        return {"status": "SUCCESS", "bronze_output_path": path, "records": records, "source": source}

    def process_bronze_to_silver(self, records: List[Dict[str, Any]], symbol: str, date_str: str, source: str = "v1.0.0_api_klines") -> Dict[str, Any]:
        if not records:
            raise ValueError("Cannot transform an empty Bronze batch")
        fields = ("symbol", "timestamp", "open", "high", "low", "close", "volume")
        cleaned = [{field: row[field] for field in fields} for row in records]
        path = self._write(self.silver_bucket, f"klines/symbol={symbol}/date={date_str}/silver.json", {"symbol": symbol, "source": source, "records": cleaned})
        return {"status": "SUCCESS", "silver_output_path": path, "cleaned_ticks": len(cleaned), "records": cleaned, "source": source}

    def write_market_metrics(self, bucket, dataset, run_key, records):
        source_map = {
            "klines": "v1.0.0_api_klines",
            "agg_trades": "v1.0.0_api_aggtrade",
            "order_book_depth": "v1.0.0_api_depth",
            "futures_liquidations": "v1.0.0_api_liquidations"
        }
        source_tag = source_map.get(dataset, f"v1.0.0_api_{dataset}")
        return self._write(bucket, f"market_metrics/{dataset}/run={run_key}/data.json", {"dataset": dataset, "source": source_tag, "records": records})
    def read_market_metrics(self, path):
        if not path.startswith("s3://"): raise ValueError(f"Invalid MinIO path: {path}")
        bucket, obj = path[5:].split("/", 1); response = self._client().get_object(bucket, obj)
        try: return json.loads(response.read().decode("utf-8"))["records"]
        finally: response.close(); response.release_conn()

    def clean_expired_bronze_files(self, retention_days: int = 30) -> Dict[str, Any]:
        """Delete only Bronze objects older than the configured retention period."""
        if retention_days <= 0:
            raise ValueError("retention_days must be greater than zero")
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        files_purged = reclaimed_bytes = 0
        try:
            client = self._client()
            if not client.bucket_exists(self.bronze_bucket):
                return {"bucket": self.bronze_bucket, "retention_days": retention_days, "files_purged": 0, "reclaimed_bytes": 0, "status": "SUCCESS"}
            for obj in client.list_objects(self.bronze_bucket, recursive=True):
                modified = obj.last_modified
                if modified.tzinfo is None:
                    modified = modified.replace(tzinfo=timezone.utc)
                if modified >= cutoff:
                    continue
                client.remove_object(self.bronze_bucket, obj.object_name)
                files_purged += 1
                reclaimed_bytes += obj.size or 0
        except Exception as exc:
            raise RuntimeError(f"MinIO Bronze cleanup failed: {exc}") from exc
        return {"bucket": self.bronze_bucket, "retention_days": retention_days, "files_purged": files_purged, "reclaimed_bytes": reclaimed_bytes, "status": "SUCCESS"}
