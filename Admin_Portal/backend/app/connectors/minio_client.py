import logging
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from typing import Dict, Any, List
from app.config import settings

logger = logging.getLogger(__name__)

class MinIOConnector:
    def __init__(self):
        self.endpoint = f"http://{settings.MINIO_ENDPOINT}"
        self.access_key = settings.MINIO_ACCESS_KEY
        self.secret_key = settings.MINIO_SECRET_KEY
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(
                    signature_version="s3v4",
                    connect_timeout=2,
                    read_timeout=2,
                    retries={"max_attempts": 1}
                ),
                region_name="us-east-1"
            )
        return self._client

    def get_health(self) -> Dict[str, Any]:
        """Check if MinIO S3 API is responding."""
        try:
            buckets = self.client.list_buckets()
            names = [b["Name"] for b in buckets.get("Buckets", [])]
            return {
                "online": True,
                "status": "ONLINE",
                "buckets": names
            }
        except Exception as e:
            logger.debug("MinIO ping failed: %s", e)
            return {
                "online": False,
                "status": "OFFLINE",
                "error": "Cannot reach MinIO S3 API (9000)"
            }

    def get_storage_overview(self) -> Dict[str, Any]:
        """Calculates storage footprint and layer metrics across buckets."""
        overview = {
            "online": False,
            "total_objects": 0,
            "total_bytes": 0,
            "buckets": {}
        }
        try:
            buckets_resp = self.client.list_buckets()
            overview["online"] = True
            for b in buckets_resp.get("Buckets", []):
                b_name = b["Name"]
                obj_count = 0
                byte_sum = 0
                try:
                    paginator = self.client.get_paginator('list_objects_v2')
                    page_iterator = paginator.paginate(Bucket=b_name, PaginationConfig={'MaxItems': 1000})
                    for page in page_iterator:
                        for item in page.get("Contents", []):
                            obj_count += 1
                            byte_sum += item["Size"]
                except Exception:
                    pass
                overview["buckets"][b_name] = {
                    "count": obj_count,
                    "bytes": byte_sum
                }
                overview["total_objects"] += obj_count
                overview["total_bytes"] += byte_sum
            return overview
        except Exception as e:
            logger.debug("Failed to calculate storage overview: %s", e)
            return overview

    def list_objects(self, bucket: str = settings.MINIO_BRONZE_BUCKET, prefix: str = "", max_keys: int = 50) -> List[Dict[str, Any]]:
        """List objects in bucket with size and timestamp, sorted by newest first."""
        try:
            resp = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
            items = []
            for obj in resp.get("Contents", []):
                items.append({
                    "key": obj["Key"],
                    "size_bytes": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                    "storage_class": obj.get("StorageClass", "STANDARD")
                })
            # Sort newest first so operators always see the latest ingested files
            items.sort(key=lambda x: x.get("last_modified", ""), reverse=True)
            return items[:max_keys]
        except Exception as e:
            logger.debug("Failed to list MinIO objects: %s", e)
            return []

    def generate_presigned_download_url(self, bucket: str, object_key: str, expires_in: int = 300) -> str:
        """Generate a secure, time-limited pre-signed download URL."""
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": object_key},
                ExpiresIn=expires_in
            )
            return url
        except Exception as e:
            logger.error("Failed to generate presigned URL: %s", e)
            return ""

minio_connector = MinIOConnector()
