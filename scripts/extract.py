import pandas as pd
import io
import logging
from s3_utils import get_s3_client
from logger_config import setup_logger
logger = setup_logger(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def read_csv_from_s3(bucket_name, file_key, delimiter=","):
    """Generic CSV reader from S3, returns DataFrame."""
    try:
        s3 = get_s3_client()
        logger.info(f"Attempting to read {file_key} from bucket {bucket_name}")
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = obj["Body"].read().decode("utf-8")
        df = pd.read_csv(io.StringIO(data), delimiter=delimiter)
        logging.info(f"Successfully read {len(df)} rows from {file_key}")
        return df
    except s3.exceptions.NoSuchKey:
        logger.error(f"File '{file_key}' not found in bucket '{bucket_name}'")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error reading {file_key}: {e}")
        raise
    #except Exception as e:
    #    logging.error(f"Error reading {file_key} from S3", exc_info=True)
    #    return pd.DataFrame()
def list_bucket_objects(bucket_name):
    """List all objects in a bucket to help debug file names."""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket_name)
    print([obj["Key"] for obj in response.get("Contents", [])])