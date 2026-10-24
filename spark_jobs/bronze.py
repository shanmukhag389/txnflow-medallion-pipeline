"""Bronze layer: read one day of raw JSON, add metadata, write Parquet to S3."""
import argparse

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RAW_SCHEMA = StructType([
    StructField("txn_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("amount", StringType()),  # kept as text: bronze stores data as received
    StructField("currency", StringType()),
    StructField("status", StringType()),
    StructField("txn_ts", StringType()),
    StructField("_corrupt_record", StringType()),
])


def get_spark():
    # Credentials come from your ~/.aws setup at runtime, never from the code.
    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()
    region = session.region_name or "ap-southeast-2"
    return (
        SparkSession.builder.master("local[2]").appName("txnflow-bronze")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.access.key", creds.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--bucket", default="txnflow-medallion-pipeline")
    args = p.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    src = f"s3a://{args.bucket}/raw/dt={args.date}/"
    dst = f"s3a://{args.bucket}/bronze"

    df = (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(src)
    )
    bronze = (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("dt", F.lit(args.date))
    )
    # dynamic overwrite: re-running a date replaces only that date's folder
    bronze.write.mode("overwrite").partitionBy("dt").parquet(dst)

    written = spark.read.parquet(f"{dst}/dt={args.date}")
    print(f"Bronze rows written for {args.date}: {written.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
