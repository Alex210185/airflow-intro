from pyspark.sql import DataFrame

def read_csv_from_s3(spark, bucket: str, key: str) -> DataFrame:
    path = f"s3://{bucket}/{key}"
    return spark.read.option("header", True).csv(path)

def read_parquet_from_s3(spark, bucket: str, key: str) -> DataFrame:
    path = f"s3://{bucket}/{key}"
    return spark.read.parquet(path)

def read_json_from_s3(spark, bucket: str, key: str) -> DataFrame:
    path = f"s3://{bucket}/{key}"
    return spark.read.json(path)
