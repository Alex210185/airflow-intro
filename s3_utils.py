import boto3
import logging
import os
import io
import pandas as pd
from extraction import aws_config   # import config safely
from logger_config import setup_logger

logger = setup_logger(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_s3_client():
    """Return a boto3 S3 client using config."""
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-2"
    )

def write_dicts_to_s3_parquet(records: list[dict], bucket_name: str, file_key_prefix: str, partition_key: str = None):
    """Write list of dicts to Parquet and upload to S3, optionally partitioned by a column."""
    try:
        if not records:
            logger.warning(f"No records to write for {file_key_prefix}")
            return

        df = pd.DataFrame(records)

        # Optional partitioning by a column
        if partition_key and partition_key in df.columns:
            unique_values = df[partition_key].unique()
            for value in unique_values:
                subset = df[df[partition_key] == value]
                parquet_buffer = io.BytesIO()
                subset.to_parquet(parquet_buffer, index=False, compression="snappy")

                partition_path = f"{file_key_prefix}/{partition_key}={value}/data.parquet"
                s3 = get_s3_client()
                s3.put_object(Bucket=bucket_name, Key=partition_path, Body=parquet_buffer.getvalue())
                logger.info(f"Partition written: s3://{bucket_name}/{partition_path}")
        else:
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, compression="snappy")
            s3 = get_s3_client()
            s3.put_object(Bucket=bucket_name, Key=f"{file_key_prefix}/data.parquet", Body=parquet_buffer.getvalue())
            logger.info(f"File written: s3://{bucket_name}/{file_key_prefix}/data.parquet")

    except Exception as e:
        logger.exception(f"Error writing {file_key_prefix} to S3: {e}")
        raise


def write_items_partitioned(records: list[dict], bucket_name: str, file_key_prefix: str):
    """Write items data partitioned by Year and Month from Purchase Date."""
    try:
        if not records:
            logger.warning(f"No records to write for {file_key_prefix}")
            return

        df = pd.DataFrame(records)

        # Ensure Purchase Date is datetime
        df["PurchaseDate"] = pd.to_datetime(df["PurchaseDate"], errors="coerce")
        df = df.dropna(subset=["PurchaseDate"])

        # Add Year and Month columns
        df["Year"] = df["PurchaseDate"].dt.year
        df["Month"] = df["PurchaseDate"].dt.month

        # Partition by Year/Month
        for (year, month), subset in df.groupby(["Year", "Month"]):
            parquet_buffer = io.BytesIO()
            subset.to_parquet(parquet_buffer, index=False, compression="snappy")

            partition_path = f"{file_key_prefix}/Year={year}/Month={month}/data.parquet"
            s3 = get_s3_client()
            s3.put_object(Bucket=bucket_name, Key=partition_path, Body=parquet_buffer.getvalue())
            logger.info(f"Partition written: s3://{bucket_name}/{partition_path}")

    except Exception as e:
        logger.exception(f"Error writing partitioned items to S3: {e}")
        raise


def list_bucket_objects(bucket_name):
    """List all objects in a bucket to help debug file names."""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket_name)
    print([obj["Key"] for obj in response.get("Contents", [])])
