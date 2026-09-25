"""Unit tests for idempotent local MinIO bucket initialization."""

from scripts.init_minio_bucket import ensure_bucket


class FakeMinio:
    def __init__(self, exists: bool) -> None:
        self.exists = exists
        self.created: list[str] = []

    def bucket_exists(self, bucket: str) -> bool:
        return self.exists

    def make_bucket(self, bucket: str) -> None:
        self.created.append(bucket)


def test_ensure_bucket_creates_missing_bucket() -> None:
    client = FakeMinio(exists=False)
    assert ensure_bucket(client, "taxi") is True
    assert client.created == ["taxi"]


def test_ensure_bucket_skips_existing_bucket() -> None:
    client = FakeMinio(exists=True)
    assert ensure_bucket(client, "taxi") is False
    assert client.created == []
