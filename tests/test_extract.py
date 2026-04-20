import io
import pandas as pd
import pytest
from extraction.extract import read_csv_from_s3

def test_read_csv_from_s3(monkeypatch):
    # Mock S3 client
    class MockS3:
        def get_object(self, Bucket, Key):
            # Simulate a CSV file with two rows
            return {"Body": io.BytesIO(b"State\nCA\nNY")}

    # Patch get_s3_client inside extraction.extract
    monkeypatch.setattr("extraction.extract.get_s3_client", lambda: MockS3())

    df = read_csv_from_s3("fake-bucket", "customers.csv")
    assert isinstance(df, pd.DataFrame)
    assert "State" in df.columns
    assert len(df) == 2
