import io
import pandas as pd
import pytest
from s3_utils import get_s3_client, write_dicts_to_s3_parquet, list_bucket_objects

# --- Fixtures / Mocks ---
class MockS3Client:
    def __init__(self):
        self.storage = {}

    def put_object(self, Bucket, Key, Body):
        # Simulate writing to S3
        self.storage[(Bucket, Key)] = Body
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def list_objects_v2(self, Bucket):
        # Simulate listing objects
        contents = [{"Key": key} for (bucket, key) in self.storage.keys() if bucket == Bucket]
        return {"Contents": contents} if contents else {}

@pytest.fixture
def mock_s3(monkeypatch):
    client = MockS3Client()
    monkeypatch.setattr("s3_utils.get_s3_client", lambda: client)
    return client

# --- Tests ---
def test_write_dicts_to_s3_parquet_basic(mock_s3):
    records = [{"State": "CA"}, {"State": "NY"}]
    write_dicts_to_s3_parquet(records, "test-bucket", "customers/valid")

    # Verify file was written
    keys = [key for (bucket, key) in mock_s3.storage.keys()]
    assert "customers/valid/data.parquet" in keys[0]

def test_write_dicts_to_s3_parquet_partitioned(mock_s3):
    records = [{"State": "CA"}, {"State": "NY"}, {"State": "CA"}]
    write_dicts_to_s3_parquet(records, "test-bucket", "customers/valid", partition_key="State")

    # Verify partitions were created
    keys = [key for (bucket, key) in mock_s3.storage.keys()]
    assert any("State=CA" in key for key in keys)
    assert any("State=NY" in key for key in keys)

def test_list_bucket_objects(mock_s3, capsys):
    # Preload mock storage
    mock_s3.put_object("test-bucket", "customers/valid/data.parquet", b"fake")
    list_bucket_objects("test-bucket")

    captured = capsys.readouterr()
    assert "customers/valid/data.parquet" in captured.out
