from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestS3Read") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.defaultFS", "s3a://source-bucket-230525")\
    .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.retry.interval", "1000") \
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "50") \
    .getOrCreate()

df = spark.read.csv("s3a://source-bucket-230525/customers.csv", header=True)

df.show()

