from pyspark.sql import DataFrame

def write_parquet(df: DataFrame, bucket: str, prefix: str):
    path = f"s3://{bucket}/silver/{prefix}"
    df.write.mode("overwrite").parquet(path)
