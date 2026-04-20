import logging
from extraction.extract import read_csv_from_s3
from extraction.validate import validate_customers, validate_items
from s3_utils import write_dicts_to_s3_parquet
from extraction.extract import read_csv_from_s3, list_bucket_objects
from s3_utils import write_dicts_to_s3_parquet, write_items_partitioned
# Configure logging for this module
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("pipeline")

def run_pipeline():
    
    source_bucket = "source-bucket-230525"
    target_bucket = "target-bucket-230525"

    logger.info("🚀 Starting pipeline execution")
    logger.info(f"Source bucket: {source_bucket} | Target bucket: {target_bucket}")

 # Debug: list files before extraction
    logger.info("Listing files in source bucket...")
    list_bucket_objects(source_bucket)
    # Step 1: Extract
    logger.info("Step 1: Extracting data from S3")
    customers_df = read_csv_from_s3(source_bucket, "customers.csv", delimiter=",")
    items_df = read_csv_from_s3(source_bucket, "Items.csv", delimiter=";")
    
    logger.info(f"Customers extracted: {len(customers_df)} rows")
    logger.info(f"Items extracted: {len(items_df)} rows")

    # Step 2: Validate
    logger.info("Step 2: Validating data")
    valid_customers, invalid_customers = validate_customers(customers_df)
    valid_items, invalid_items = validate_items(items_df)

    logger.info(f"Valid customers: {len(valid_customers)} | Invalid customers: {len(invalid_customers)}")
    logger.info(f"Valid items: {len(valid_items)} | Invalid items: {len(invalid_items)}")

    # Step 3: Load
    logger.info("Step 3: Writing validated data to S3")
    try:
        # Partition items by Satates
        write_dicts_to_s3_parquet(valid_customers.to_dict(orient="records"),target_bucket, "customers/valid")
        write_dicts_to_s3_parquet(invalid_customers.to_dict(orient="records"),target_bucket, "customers/invalid")
        
        # Partition items by Year/Month from Purchase Date
        write_items_partitioned(valid_items.to_dict(orient="records"),target_bucket, "items/valid")
        write_items_partitioned(invalid_items.to_dict(orient="records"),target_bucket, "items/invalid")

    except Exception as e:
        logger.exception(f"Error during load step: {e}")

    print("✅ Pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()
