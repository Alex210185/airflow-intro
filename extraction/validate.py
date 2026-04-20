# validate.py
import logging
from logger_config import setup_logger
logger = setup_logger(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def validate_customers(customers_df):
    """Validate customers: State must be CA."""
    valid = customers_df[customers_df["State"].str.strip().str.upper() == "CA"]
    invalid = customers_df[customers_df["State"].str.strip().str.upper() != "CA"]

    logging.info(f"Valid customers (State=CA): {len(valid)}")
    logging.info(f"Invalid customers (State!=CA): {len(invalid)}")
    return valid, invalid


def validate_items(items_df):
    """Validate items: CustomerID must not be null."""
    valid = items_df[items_df["CustomerID"].notnull()]
    invalid = items_df[items_df["CustomerID"].isnull()]

    logging.info(f"Valid items: {len(valid)}")
    logging.info(f"Invalid items (missing CustomerID): {len(invalid)}")
    return valid, invalid
