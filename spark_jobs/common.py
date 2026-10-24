"""Shared Spark session setup for all TxnFlow jobs."""
import boto3
from pyspark.sql import SparkSession


def get_spark(app_name):
    # Credentials come from ~/.aws at runtime, never from the code.
    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()
    region = session.region_name or "ap-southeast-2"
    spark = (
        SparkSession.builder.master("local[2]").appName(app_name)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.access.key", creds.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
