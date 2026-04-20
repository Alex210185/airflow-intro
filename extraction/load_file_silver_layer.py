import boto3
import logging
import aws_config
import pandas as pd
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------
# Customer Section
# ---------------------------

def write_customers_to_s3_parquet(customers_list, bucket_name, file_key):
    """
    Write a list of dictionaries to Parquet format and upload to S3.
    """
    try:
        if not customers_list:
            logging.warning("No customers to write.")
            return

        # Convert list of dicts to DataFrame
        df = pd.DataFrame(customers_list)

        # Write DataFrame to in-memory Parquet buffer
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        # Connect to S3
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=aws_config.AWS_SECRET_ACCESS_KEY,
            region_name=aws_config.AWS_REGION
        )

        # Upload buffer to S3
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=parquet_buffer.getvalue())
        logging.info(f"Parquet file successfully written to s3://{bucket_name}/{file_key}")

    except Exception as e:
        logging.error("Error while writing customers to Parquet in S3", exc_info=True)
# ---------------------------
# Items Section
# ---------------------------
def write_items_to_s3_parquet(items_list, bucket_name, file_key):
    """
    Write a list of dictionaries to Parquet format and upload to S3.
    """
    try:
        if not items_list:
            logging.warning("No items to write.")
            return

        # Convert list of dicts to DataFrame
        df = pd.DataFrame(items_list)

        # Write DataFrame to in-memory Parquet buffer
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        # Connect to S3
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=aws_config.AWS_SECRET_ACCESS_KEY,
            region_name=aws_config.AWS_REGION
        )

        # Upload buffer to S3
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=parquet_buffer.getvalue())
        logging.info(f"Parquet file successfully written to s3://{bucket_name}/{file_key}")

    except Exception as e:
        logging.error("Error while writing items to Parquet in S3", exc_info=True)