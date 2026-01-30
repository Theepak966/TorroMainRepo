"""
AWS S3 client for Torro discovery.
Provides list_buckets, list_objects, and test_connection following the same
interfaces as the Azure Blob client for consistency across connectors.
"""

from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = Exception
    NoCredentialsError = Exception


def create_s3_client(config: Dict[str, Any]) -> "S3Client":
    """
    Create an S3Client from connection config.
    Supports Access Key auth: aws_access_key_id, aws_secret_access_key, region_name.
    """
    if not BOTO3_AVAILABLE:
        raise ImportError(
            "boto3 is required for AWS S3 support. Install with: pip install boto3"
        )

    access_key = config.get("aws_access_key_id")
    secret_key = config.get("aws_secret_access_key")
    region = config.get("region_name") or config.get("region") or "us-east-1"

    if not access_key or not secret_key:
        raise ValueError(
            "AWS S3 requires aws_access_key_id and aws_secret_access_key. "
            "Provide both in the connection configuration."
        )

    return S3Client(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


class S3Client:
    """
    S3 client for listing buckets and objects.
    Mirrors Azure Blob client semantics (list_containers -> list_buckets,
    list_blobs -> list_objects) for uniform discovery logic.
    """

    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str = "us-east-1",
    ) -> None:
        self._client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        self._region = region_name
        logger.info(
            "FN:create_s3_client message:Using Access Key authentication region:{}".format(
                region_name
            )
        )

    def list_buckets(self) -> List[Dict[str, Any]]:
        """
        List all S3 buckets. Return shape matches Azure list_containers:
        [{"name": str, "creation_date": str|None, ...}]
        """
        try:
            response = self._client.list_buckets()
            buckets = []
            for b in response.get("Buckets", []):
                buckets.append({
                    "name": b["Name"],
                    "creation_date": (
                        b["CreationDate"].isoformat()
                        if b.get("CreationDate") else None
                    ),
                })
            logger.info("FN:list_buckets bucket_count:{}".format(len(buckets)))
            return buckets
        except ClientError as e:
            logger.error("FN:list_buckets error:{}".format(str(e)))
            raise
        except NoCredentialsError as e:
            logger.error("FN:list_buckets credentials_error:{}".format(str(e)))
            raise ValueError(
                "AWS credentials invalid or missing. "
                "Check aws_access_key_id and aws_secret_access_key."
            ) from e

    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
        max_keys: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        List objects in a bucket, optionally under a prefix.
        Returns list of dicts with name, full_path, size, last_modified, etag, etc.
        Uses pagination to retrieve all objects.
        """
        prefix = (prefix or "").strip().rstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        objects: List[Dict[str, Any]] = []
        paginator = self._client.get_paginator("list_objects_v2")
        page_params: Dict[str, Any] = {"Bucket": bucket_name}
        if prefix:
            page_params["Prefix"] = prefix

        try:
            for page in paginator.paginate(**page_params):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith("/"):
                        continue
                    name = key.split("/")[-1]
                    owner_info = obj.get("Owner") or {}
                    owner_id = owner_info.get("ID") if isinstance(owner_info, dict) else None
                    objects.append({
                        "name": name,
                        "full_path": key,
                        "key": key,
                        "size": obj.get("Size", 0) or 0,
                        "last_modified": obj.get("LastModified"),
                        "etag": (obj.get("ETag") or "").strip('"'),
                        "storage_class": obj.get("StorageClass"),
                        "owner_id": owner_id,
                    })
                    if max_keys is not None and len(objects) >= max_keys:
                        logger.info(
                            "FN:list_objects bucket:{} prefix:{} message:Reached max_keys:{}".format(
                                bucket_name, prefix, max_keys
                            )
                        )
                        return objects

            logger.info(
                "FN:list_objects bucket:{} prefix:{} object_count:{}".format(
                    bucket_name, prefix or "root", len(objects)
                )
            )
            return objects
        except ClientError as e:
            logger.error(
                "FN:list_objects bucket:{} prefix:{} error:{}".format(
                    bucket_name, prefix, str(e)
                )
            )
            raise
        except NoCredentialsError as e:
            logger.error("FN:list_objects credentials_error:{}".format(str(e)))
            raise ValueError("AWS credentials invalid or missing.") from e

    def get_bucket_location(self, bucket_name: str) -> Optional[str]:
        """
        Get the AWS region where the bucket is located.
        Returns None on error. Note: us-east-1 returns None from API; caller should treat None as 'us-east-1'.
        """
        try:
            r = self._client.get_bucket_location(Bucket=bucket_name)
            loc = r.get("LocationConstraint")
            if loc is None or loc == "":
                return "us-east-1"
            return str(loc)
        except ClientError as e:
            logger.warning("FN:get_bucket_location bucket:{} error:{}".format(bucket_name, str(e)))
            return None
        except NoCredentialsError:
            return None

    def get_bucket_owner(self, bucket_name: str) -> Optional[str]:
        """
        Get the bucket owner ID (canonical user ID) via GetBucketAcl.
        Used as fallback when ListObjectsV2 does not return Owner per object.
        """
        try:
            r = self._client.get_bucket_acl(Bucket=bucket_name)
            owner = r.get("Owner") or {}
            return owner.get("ID") if isinstance(owner, dict) else None
        except ClientError as e:
            logger.warning("FN:get_bucket_owner bucket:{} error:{}".format(bucket_name, str(e)))
            return None
        except NoCredentialsError:
            return None

    def head_object(self, bucket_name: str, key: str) -> Dict[str, Any]:
        """Get object metadata (size, etc.) via HeadObject."""
        try:
            r = self._client.head_object(Bucket=bucket_name, Key=key)
            return {
                "size": r.get("ContentLength", 0) or 0,
                "etag": (r.get("ETag") or "").strip('"'),
                "last_modified": r.get("LastModified"),
                "content_type": r.get("ContentType", "application/octet-stream"),
            }
        except ClientError as e:
            logger.error("FN:head_object bucket:{} key:{} error:{}".format(bucket_name, key, str(e)))
            raise

    def get_object_range(self, bucket_name: str, key: str, offset: int, length: int) -> bytes:
        """Download a byte range from an S3 object."""
        try:
            r = self._client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range="bytes={}-{}".format(offset, offset + length - 1),
            )
            return r["Body"].read()
        except ClientError as e:
            logger.error("FN:get_object_range bucket:{} key:{} offset:{} len:{} error:{}".format(
                bucket_name, key, offset, length, str(e)
            ))
            raise

    def get_object_full(self, bucket_name: str, key: str, max_bytes: Optional[int] = None) -> bytes:
        """Download full object or first max_bytes if specified."""
        try:
            if max_bytes is not None and max_bytes > 0:
                r = self._client.get_object(Bucket=bucket_name, Key=key, Range="bytes=0-{}".format(max_bytes - 1))
            else:
                r = self._client.get_object(Bucket=bucket_name, Key=key)
            return r["Body"].read()
        except ClientError as e:
            logger.error("FN:get_object_full bucket:{} key:{} error:{}".format(bucket_name, key, str(e)))
            raise

    def get_parquet_footer(self, bucket_name: str, key: str, footer_size_kb: int = 256) -> bytes:
        """
        Download the parquet footer (last N bytes). Schema lives in the footer.
        Uses same strategy as Azure: last 8 bytes for footer length, then exact footer, with
        progressive fallback if PAR1 not found.
        """
        import struct

        try:
            info = self.head_object(bucket_name, key)
            file_size = int(info.get("size") or 0)
            if file_size == 0:
                logger.warning("FN:get_parquet_footer key:{} message:File size is 0".format(key))
                return b""
            if file_size < 8:
                return self.get_object_full(bucket_name, key)

            max_footer_bytes = footer_size_kb * 1024
            last_8 = self.get_object_range(bucket_name, key, file_size - 8, 8)
            if len(last_8) < 8:
                logger.warning("FN:get_parquet_footer key:{} message:Could not read last 8 bytes".format(key))
                return self.get_object_full(bucket_name, key)

            if last_8[-4:] != b"PAR1":
                for attempt_kb in [512, 1024, 256, 2048]:
                    attempt_bytes = attempt_kb * 1024
                    if file_size <= attempt_bytes:
                        return self.get_object_full(bucket_name, key)
                    try:
                        chunk = self.get_object_range(
                            bucket_name, key,
                            max(0, file_size - attempt_bytes),
                            min(attempt_bytes, file_size),
                        )
                        if len(chunk) >= 4 and chunk[-4:] == b"PAR1":
                            return chunk
                    except Exception as e:
                        logger.debug("FN:get_parquet_footer key:{} attempt_kb:{} error:{}".format(key, attempt_kb, str(e)))
                logger.warning("FN:get_parquet_footer key:{} message:PAR1 not found, downloading full file".format(key))
                return self.get_object_full(bucket_name, key)

            # Wide Parquet tables (hundreds+ columns, many row-groups) can have very large footers.
            # Allow up to 32MB when file reports it to avoid truncating the footer and losing schema.
            max_footer_cap = max(max_footer_bytes, 32 * 1024 * 1024)
            try:
                footer_length = struct.unpack("<I", last_8[0:4])[0]
                if footer_length == 0 or footer_length > max_footer_cap:
                    footer_bytes = min(max_footer_bytes, max_footer_cap)
                else:
                    footer_bytes = min(footer_length + 8, max_footer_cap)
            except Exception:
                footer_bytes = max_footer_bytes

            if file_size <= footer_bytes:
                return self.get_object_full(bucket_name, key)
            return self.get_object_range(bucket_name, key, file_size - footer_bytes, footer_bytes)
        except Exception as e:
            logger.error("FN:get_parquet_footer bucket:{} key:{} error:{}".format(bucket_name, key, str(e)))
            return b""

    def get_parquet_footer_and_row_group(
        self,
        bucket_name: str,
        key: str,
        footer_size_kb: int = 256,
        row_group_size_mb: int = 2,
    ) -> bytes:
        """Footer (schema) + first row group (PII sample) for large parquets."""
        try:
            info = self.head_object(bucket_name, key)
            file_size = int(info.get("size") or 0)
            if file_size == 0:
                return b""
            footer_data = self.get_parquet_footer(bucket_name, key, footer_size_kb=footer_size_kb)
            if not footer_data or len(footer_data) < 8:
                return footer_data or b""

            rg_bytes = row_group_size_mb * 1024 * 1024
            try:
                row_group_data = self.get_object_range(bucket_name, key, 0, min(rg_bytes, file_size))
            except Exception as e:
                logger.warning("FN:get_parquet_footer_and_row_group key:{} row_group_error:{}".format(key, str(e)))
                return footer_data

            combined = row_group_data + footer_data
            if len(combined) >= 4 and combined[-4:] == b"PAR1":
                return combined
            return footer_data
        except Exception as e:
            logger.error("FN:get_parquet_footer_and_row_group bucket:{} key:{} error:{}".format(bucket_name, key, str(e)))
            try:
                return self.get_parquet_footer(bucket_name, key, footer_size_kb=footer_size_kb)
            except Exception:
                return b""

    def get_parquet_file_for_extraction(
        self,
        bucket_name: str,
        key: str,
        max_size_mb: int = 5,
    ) -> bytes:
        """Full file or first max_size_mb for schema + PII. For small parquets."""
        try:
            info = self.head_object(bucket_name, key)
            file_size = int(info.get("size") or 0)
            max_bytes = max_size_mb * 1024 * 1024
            if file_size > 100 * 1024 * 1024:
                max_bytes = min(max_bytes, 5 * 1024 * 1024)
            if file_size <= max_bytes:
                return self.get_object_full(bucket_name, key)
            return self.get_object_range(bucket_name, key, 0, max_bytes)
        except Exception as e:
            logger.warning("FN:get_parquet_file_for_extraction bucket:{} key:{} error:{}".format(bucket_name, key, str(e)))
            return b""

    def test_connection(self) -> Dict[str, Any]:
        """
        Verify credentials by listing buckets.
        Returns {success: bool, message: str, container_count: int} for API parity.
        """
        try:
            buckets = self.list_buckets()
            return {
                "success": True,
                "message": "Connection successful",
                "container_count": len(buckets),
            }
        except ValueError as e:
            return {
                "success": False,
                "message": str(e),
                "container_count": 0,
            }
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", str(e))
            if code == "InvalidAccessKeyId":
                detail = (
                    "Invalid Access Key ID. Check aws_access_key_id "
                    "and ensure the key is active in IAM."
                )
            elif code == "SignatureDoesNotMatch":
                detail = (
                    "Invalid Secret Access Key. Check aws_secret_access_key."
                )
            else:
                detail = msg
            logger.error("FN:test_connection error:{}".format(msg))
            return {
                "success": False,
                "message": detail,
                "container_count": 0,
            }
        except Exception as e:
            logger.error("FN:test_connection error:{}".format(str(e)))
            return {
                "success": False,
                "message": "Connection test failed: {}".format(str(e)),
                "container_count": 0,
            }
