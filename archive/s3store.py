"""The LocalStore interface against real S3, or any S3-compatible server (MinIO).

Everything the design depends on maps to one S3 request:
  put_if_absent   PutObject  If-None-Match: *        412 -> False
  swap            PutObject  If-Match: <ETag>        412 / 404 -> False
  get_with_etag   GetObject, returning the server's ETag
  get_range       GetObject  Range: bytes=...
  list            ListObjectsV2 (paginated, flat prefix)
  modified        HeadObject LastModified

Note on retries: if a conditional PUT lands but the response is lost, the SDK
retry gets 412 and we report "someone else has it". For a log commit that
means the writer skips the number and the batch is visible but not
acknowledged. The station resends and dedupe collapses the copy, so this case
stays safe, just slower.
"""
import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def _status(e):
    return e.response.get("ResponseMetadata", {}).get("HTTPStatusCode"), e.response.get("Error", {}).get("Code")


class S3Store:
    def __init__(self, bucket, prefix="", endpoint=None, client=None):
        self.bucket, self.prefix = bucket, prefix
        self.s3 = client or boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(retries={"max_attempts": 3, "mode": "standard"},
                          s3={"addressing_style": "path"}))

    def _k(self, key):
        return self.prefix + key

    def ensure_bucket(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError:
            self.s3.create_bucket(Bucket=self.bucket)

    def _conditional_put(self, key, body, **cond):
        for attempt in range(5):
            try:
                self.s3.put_object(Bucket=self.bucket, Key=self._k(key), Body=body, **cond)
                return True
            except ClientError as e:
                status, code = _status(e)
                if status == 412 or code in ("PreconditionFailed", "NoSuchKey") or status == 404:
                    return False
                if status == 409:  # ConditionalRequestConflict: concurrent write in flight
                    if "IfMatch" in cond:
                        return False  # caller re-reads and decides
                    time.sleep(0.05 * (attempt + 1))
                    if hasattr(body, "seek"):
                        body.seek(0)
                    continue
                raise
        return False

    def put_if_absent(self, key, data):
        return self._conditional_put(key, data, IfNoneMatch="*")

    def put_file_if_absent(self, key, local_path):
        with open(local_path, "rb") as f:  # streamed by the SDK, not loaded
            return self._conditional_put(key, f, IfNoneMatch="*")

    def swap(self, key, expected_etag, data):
        return self._conditional_put(key, data, IfMatch=expected_etag)

    def get_with_etag(self, key):
        try:
            r = self.s3.get_object(Bucket=self.bucket, Key=self._k(key))
            return r["Body"].read(), r["ETag"]
        except ClientError as e:
            if _status(e)[0] == 404:
                return None, None
            raise

    def get(self, key):
        return self.get_with_etag(key)[0]

    def get_range(self, key, start, length=None):
        if start < 0:
            rng = f"bytes={start}"
        elif length is None:
            rng = f"bytes={start}-"
        else:
            if length <= 0:
                return b""
            rng = f"bytes={start}-{start + length - 1}"
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=self._k(key), Range=rng)["Body"].read()
        except ClientError as e:
            status, _ = _status(e)
            if status == 404:
                return None
            if status == 416:
                return b""
            raise

    def list(self, prefix):
        keys = []
        pages = self.s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=self._k(prefix) + "/")
        for page in pages:
            keys.extend(o["Key"][len(self.prefix):] for o in page.get("Contents", []))
        return sorted(keys)

    def modified(self, key):
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=self._k(key))["LastModified"].timestamp()
        except ClientError as e:
            if _status(e)[0] == 404:
                return None
            raise

    def delete(self, key):
        self.s3.delete_object(Bucket=self.bucket, Key=self._k(key))
