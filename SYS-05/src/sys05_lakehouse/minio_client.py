"""
MinIO S3 Object Storage Manager for Medallion Lakehouse Architecture
"""
import io
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error
from .config import SystemConfig, get_config

logger = logging.getLogger("sys05.minio_client")

class LakehouseStorageManager:
    def __init__(self, config: Optional[SystemConfig] = None):
        self.config = config or get_config()
        self.client = Minio(
            endpoint=self.config.minio_endpoint,
            access_key=self.config.minio_access_key,
            secret_key=self.config.minio_secret_key,
            secure=self.config.minio_secure
        )
        self.medallion_buckets = [
            self.config.bucket_bronze,
            self.config.bucket_silver,
            self.config.bucket_gold
        ]

    def ensure_buckets_exist(self) -> Dict[str, bool]:
        """Creates all 3 Medallion buckets if they don't already exist."""
        results = {}
        for bucket in self.medallion_buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
                results[bucket] = True
            except Exception as e:
                logger.error(f"Failed to check/create bucket {bucket}: {e}")
                results[bucket] = False
        return results

    def get_bucket_status(self) -> List[Dict[str, Any]]:
        """Returns details, total object count, and size for each medallion bucket."""
        self.ensure_buckets_exist()
        status_list = []
        for bucket in self.medallion_buckets:
            total_size = 0
            count = 0
            try:
                objects = self.client.list_objects(bucket, recursive=True)
                for obj in objects:
                    total_size += obj.size
                    count += 1
                status_list.append({
                    "bucket": bucket,
                    "exists": True,
                    "object_count": count,
                    "total_size_mb": round(total_size / (1024 * 1024), 2)
                })
            except Exception as e:
                status_list.append({
                    "bucket": bucket,
                    "exists": False,
                    "error": str(e)
                })
        return status_list

    def upload_bronze_raw(self, object_name: str, data: Any) -> bool:
        """Upload raw data (dict, string, or bytes) to Bronze layer."""
        if isinstance(data, (dict, list)):
            content = json.dumps(data, indent=2).encode('utf-8')
            content_type = "application/json"
        elif isinstance(data, str):
            content = data.encode('utf-8')
            content_type = "text/plain"
        elif isinstance(data, bytes):
            content = data
            content_type = "application/octet-stream"
        else:
            raise ValueError("Unsupported data format for Bronze upload")

        data_stream = io.BytesIO(content)
        try:
            self.client.put_object(
                bucket_name=self.config.bucket_bronze,
                object_name=object_name,
                data=data_stream,
                length=len(content),
                content_type=content_type
            )
            return True
        except Exception as e:
            logger.error(f"Failed to upload to Bronze {object_name}: {e}")
            return False

    def upload_silver_parquet(self, object_name: str, parquet_bytes: bytes) -> bool:
        """Upload cleaned parquet bytes to Silver layer."""
        data_stream = io.BytesIO(parquet_bytes)
        try:
            self.client.put_object(
                bucket_name=self.config.bucket_silver,
                object_name=object_name,
                data=data_stream,
                length=len(parquet_bytes),
                content_type="application/octet-stream"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to upload to Silver {object_name}: {e}")
            return False

    def upload_gold_feature(self, object_name: str, data_bytes: bytes) -> bool:
        """Upload aggregated feature dataset to Gold layer."""
        data_stream = io.BytesIO(data_bytes)
        try:
            self.client.put_object(
                bucket_name=self.config.bucket_gold,
                object_name=object_name,
                data=data_stream,
                length=len(data_bytes),
                content_type="application/octet-stream"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to upload to Gold {object_name}: {e}")
            return False

    def get_object_data(self, bucket_name: str, object_name: str) -> Optional[bytes]:
        """Fetch raw bytes of an object from S3."""
        try:
            response = self.client.get_object(bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except Exception as e:
            logger.error(f"Failed to get object {object_name} from {bucket_name}: {e}")
            return None

    def apply_retention_policy(self, bronze_max_days: int = 30) -> int:
        """Removes Bronze raw objects older than specified days."""
        cutoff_date = datetime.now() - timedelta(days=bronze_max_days)
        deleted_count = 0
        try:
            objects = self.client.list_objects(self.config.bucket_bronze, recursive=True)
            for obj in objects:
                if obj.last_modified and obj.last_modified.replace(tzinfo=None) < cutoff_date:
                    self.client.remove_object(self.config.bucket_bronze, obj.object_name)
                    deleted_count += 1
        except Exception as e:
            logger.error(f"Error applying retention policy: {e}")
        return deleted_count
