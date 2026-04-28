# main_spark.py
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
#spark = SparkSession.builder \
 #   .appName("Airflow-S3-Pipeline") \
  #  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
   # .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A") \
    #.config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    #.config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    #.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    #.getOrCreate()
spark = SparkSession.builder \
    .appName("Airflow-S3-Pipeline") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

    
# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("pipeline_spark")

def run_pipeline():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Airflow-S3-Pipeline") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    source_bucket = "s3a://source-bucket-230525"
    target_bucket = "s3a://target-bucket-230525"

    logger.info("🚀 Starting Spark pipeline execution")
    logger.info(f"Source bucket: {source_bucket} | Target bucket: {target_bucket}")

    # Step 1: Extract
    logger.info("Step 1: Extracting data from S3")
    customers_df = spark.read.csv(f"{source_bucket}/customers.csv", header=True, inferSchema=True)
    items_df = spark.read.csv(f"{source_bucket}/Items.csv", header=True, sep=";", inferSchema=True)

    logger.info(f"Customers extracted: {customers_df.count()} rows")
    logger.info(f"Items extracted: {items_df.count()} rows")

    # Step 2: Validate
    logger.info("Step 2: Validating data")
    valid_customers = customers_df.filter(col("State") == "CA").cache()
    invalid_customers = customers_df.filter(col("State") != "CA")

    valid_items = items_df.filter(col("CustomerID").isNotNull()).cache()
    invalid_items = items_df.filter(col("CustomerID").isNull())

    logger.info(f"Valid customers: {valid_customers.count()} | Invalid customers: {invalid_customers.count()}")
    logger.info(f"Valid items: {valid_items.count()} | Invalid items: {invalid_items.count()}")

    # Step 3: Load
    logger.info("Step 3: Writing validated data to S3")

    # Customers
    valid_customers.write.mode("overwrite").parquet(f"{target_bucket}/customers/valid")
    invalid_customers.write.mode("overwrite").parquet(f"{target_bucket}/customers/invalid")

    # Items with partitioning by Year/Month
    items_with_date = valid_items.withColumn("Year", year(col("PurchaseDate"))) \
                                 .withColumn("Month", month(col("PurchaseDate")))

    items_with_date.write.partitionBy("Year", "Month").mode("overwrite").parquet(f"{target_bucket}/items/valid")

    invalid_items.withColumn("Year", year(col("PurchaseDate"))) \
                 .withColumn("Month", month(col("PurchaseDate"))) \
                 .write.partitionBy("Year", "Month").mode("overwrite").parquet(f"{target_bucket}/items/invalid")

    logger.info("✅ Spark pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()
