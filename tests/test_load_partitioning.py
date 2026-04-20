import io
import pandas as pd
import pytest
from s3_utils import write_dicts_to_s3_parquet, write_items_partitioned

# --- Mock S3 client ---
class MockS3Client:
    def __init__(self):
        self.storage = {}

    def put_object(self, Bucket, Key, Body):
        self.storage[(Bucket, Key)] = Body
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

@pytest.fixture
def mock_s3(monkeypatch):
    client = MockS3Client()
    # Patch get_s3_client inside s3_utils
    monkeypatch.setattr("s3_utils.get_s3_client", lambda: client)
    return client

# --- Tests ---

def test_customers_partitioned_by_state(mock_s3):
    records = [
        {"CustomerID": 1, "State": "CA"},
        {"CustomerID": 2, "State": "NY"},
        {"CustomerID": 3, "State": "CA"},
    ]
    write_dicts_to_s3_parquet(records, "test-bucket", "customers/valid", partition_key="State")

    keys = [key for (bucket, key) in mock_s3.storage.keys()]
    assert any("State=CA" in key for key in keys)
    assert any("State=NY" in key for key in keys)

def test_items_partitioned_by_purchase_date(mock_s3):
    records = [
        {"ItemID": 101, "PurchaseDate": "2025-12-15"},
        {"ItemID": 102, "PurchaseDate": "2026-01-10"},
        {"ItemID": 103, "PurchaseDate": "2026-01-20"},
    ]
    write_items_partitioned(records, "test-bucket", "items/valid")

    keys = [key for (bucket, key) in mock_s3.storage.keys()]
    # Expect Year=2025/Month=12 and Year=2026/Month=1
    assert any("Year=2025/Month=12" in key for key in keys)
    assert any("Year=2026/Month=1" in key for key in keys)
