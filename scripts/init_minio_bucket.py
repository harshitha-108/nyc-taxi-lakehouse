"""Create the local development bucket without depending on an mc image."""

from __future__ import annotations

import logging
import os
from typing import Protocol
from urllib.parse import urlsplit

from minio import Minio

logger = logging.getLogger(__name__)


class BucketClient(Protocol):
    """The two S3 bucket operations required by initialization."""

    def bucket_exists(self, bucket: str) -> bool: ...

    def make_bucket(self, bucket: str) -> None: ...


def ensure_bucket(client: BucketClient, bucket: str) -> bool:
    """Create a missing bucket; return whether this invocation created it."""
    if client.bucket_exists(bucket):
        return False
    client.make_bucket(bucket)
    return True


def main() -> None:
    """Initialize the configured MinIO bucket using environment credentials."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    endpoint = urlsplit(os.environ["MINIO_ENDPOINT"])
    if endpoint.scheme not in {"http", "https"} or not endpoint.netloc:
        raise ValueError("MINIO_ENDPOINT must be an HTTP(S) URL")
    bucket = os.environ["MINIO_BUCKET"]
    client = Minio(
        endpoint.netloc,
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=endpoint.scheme == "https",
    )
    created = ensure_bucket(client, bucket)
    logger.info("MinIO bucket %s: %s", bucket, "created" if created else "already exists")


if __name__ == "__main__":
    main()
